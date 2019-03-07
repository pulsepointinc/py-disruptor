from unittest import TestCase

from disruptor import Disruptor, BatchConsumer, Consumer
from disruptor.disruptor import RingBuffer, RingBufferLagStats
from threading import Event
import time
import random
import logging


class TestRingBuffer(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger(cls.__name__)
        cls.logger.level = logging.DEBUG

    def test_get_set(self):
        ring = RingBuffer(5)
        # assert intial ring state with wrap-around
        for idx in range(10):
            self.assertEqual(None, ring.get(idx))
        # set some elements
        for idx in range(10):
            ring.set(idx, idx)
        # assert values
        for idx in range(5):
            self.assertEqual(idx + 5, ring.get(idx))

    def test_mget_mset(self):
        ring = RingBuffer(10)
        for idx in range(10):
            ring.set(idx, idx)

        ring.mset(0, [9, 9, 9])
        # ring is now [9,9,9,3,4,5,6,7,8,9]
        self.assertEqual([9, 9, 9, 3], ring.mget(0, 4))
        self.assertEqual([9, 9, 3, 4], ring.mget(1, 4))
        self.assertEqual([8, 9, 9, 9, 9, 3], ring.mget(8, 6))
        ring.mset(8, [1, 2, 3, 4])
        # ring is now [3,4,9,3,4,5,6,7,1,2]
        self.assertEqual([3, 4, 9, 3, 4, 5, 6, 7, 1, 2], ring.mget(0, 10))
        self.assertEqual([4, 9, 3, 4, 5, 6, 7, 1, 2, 3], ring.mget(1, 10))
        ring.mset(11, [0, 1, 2])
        # ring is now [3,0,1,2,4,5,6,7,1,2]
        self.assertEqual([0, 1, 2, 4], ring.mget(11, 4))

    # This test is skipped as it is a benchmarking only test
    def test_mperf(self):
        """
        Sanity test to make sure that mget/mset impelementations of array slicing are faster than
        basic index by index fetch/updates
        """
        def inc_mget(ring, idx, count):
            ret = []
            end_idx = idx + count
            while idx < end_idx:
                ret.append(ring.get(idx))
                idx = idx + 1
            return ret

        def inc_mset(ring, idx, elements):
            c = 0
            for e in elements:
                ring.set(idx + c, e)
                c = c + 1
        # time performance
        iterations = 200000
        set_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        ring = RingBuffer(10)

        start = time.time()
        for t in range(iterations):
            ring.mset(t, set_list)
            self.assertEqual(set_list, ring.mget(t, 10))
        m_runtime = time.time() - start

        start = time.time()
        for t in range(iterations):
            inc_mset(ring, t, set_list)
            self.assertEqual(set_list, inc_mget(ring, t, 10))
        inc_runtime = time.time() - start

        self.logger.debug(
            'For {} iterations, mget/mset: {}s, inc_mget/inc_mset: {}s'.format(iterations, m_runtime, inc_runtime))

        self.assertTrue(m_runtime < inc_runtime)


class TestRingBufferLagStats(TestCase):
    def test_avg(self):
        s = RingBufferLagStats()
        s.sample(1)
        self.assertEqual(1, s.avg_lag)
        s.sample(3)
        self.assertEqual(2, s.avg_lag)
        s.sample(2)
        self.assertEqual(2, s.avg_lag)


class MemoryConsumer(BatchConsumer):
    def __init__(self, name, batch_size):
        super(MemoryConsumer, self).__init__(batch_size)
        self.consumed = []
        self.name = name

    def consume_batch(self, elements):
        self.consumed = self.consumed + elements

    def __str__(self):
        return str(self.name)


class BrokenConsumer(Consumer):
    exception = Exception('I am broken')

    def consume(self, elements):
        raise self.exception


class TestBatchConsumer(TestCase):
    def test_batching(self):
        c = MemoryConsumer('test', batch_size=3)
        # consume less than batch size and verify nothing happens
        c.consume([1])
        self.assertEqual([], c.consumed)
        # consume up to batch size and verify consumption occurs
        c.consume([2, 3])
        self.assertEqual([1, 2, 3], c.consumed)
        # consume over batch size and verify consumption occurs
        c.consume([4, 5, 6, 7, 8])
        self.assertEqual([1, 2, 3, 4, 5, 6], c.consumed)
        # close consumer and verify remainder of elements is consumed
        c.close()
        self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8], c.consumed)


class TestDisruptor(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDisruptor, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger(cls.__name__)
        cls.logger.level = logging.DEBUG
        cls.maxDiff = None

    def random_batch_iterator(self, iterable, min_batch_size, max_batch_size):
        batch_size = random.randint(min_batch_size, max_batch_size)
        batch = []
        for e in iterable:
            batch.append(e)
            if len(batch) >= batch_size:
                yield batch
                batch_size = random.randint(min_batch_size, max_batch_size)
                batch = []
        if len(batch) > 0:
            yield batch

    def test_error_handling(self):
        handled_errors = []

        def error_handler(consumer, elements, error):
            handled_errors.append({
                'consumer': consumer,
                'elements': elements,
                'error': error
            })
        disruptor = Disruptor(1, consumer_error_handler=error_handler)
        broken_consumer = BrokenConsumer()
        working_consumer = MemoryConsumer('Working Consumer', 3)
        try:
            disruptor.register_consumer(broken_consumer)
            disruptor.register_consumer(working_consumer)
            disruptor.produce(['Test element'])
        finally:
            disruptor.close()
        # verify working consumer processed elements
        self.assertEqual(['Test element'], working_consumer.consumed)
        # verify error handler was called when broken consumer failed to consume a batch of elements
        self.assertEqual([{
            'consumer': broken_consumer,
            'elements': ['Test element'],
            'error': broken_consumer.exception
        }], handled_errors)

    def test_disruptor(self):
        elements_to_produce = 10000
        min_batch_size = 1
        max_batch_size = 12
        ring_size = 8
        n_consumers = 3

        # make some consumers with variable batch sizes
        consumers = [MemoryConsumer(i, i+2) for i in range(n_consumers)]
        # make a disruptor
        disruptor = Disruptor(ring_size)
        try:
            # register consumers
            for consumer in consumers:
                disruptor.register_consumer(consumer)
            # make an array of elements to produce
            to_produce = [x for x in range(elements_to_produce)]
            # make a random size batched iterator of elements
            rnd_batch = self.random_batch_iterator(
                to_produce, min_batch_size=min_batch_size, max_batch_size=max_batch_size)
            # produce!
            for batch in rnd_batch:
                disruptor.produce(batch)
        finally:
            # shut down disruptor
            disruptor.close()

        # assert stats
        self.assertEqual(elements_to_produce, disruptor.stats.produced)
        self.logger.info(str(disruptor.stats))

        expected_results = to_produce
        # assert consumption
        for consumer in consumers:
            self.assertEqual(expected_results, consumer.consumed)

    def test_batch_alignment(self):
        # this verifies that batches that align perfectly do not hang disruptor.close
        consumer = MemoryConsumer('test', batch_size=1)
        # patch consumer with a latch that gets hit after it consumes any elements
        consumed_latch = Event()
        consume_original = consumer.consume

        def patched_consume(elements):
            try:
                return consume_original(elements)
            finally:
                consumed_latch.set()
        consumer.consume = patched_consume

        disruptor = Disruptor(1)
        try:
            disruptor.register_consumer(consumer)
            # produce an element
            disruptor.produce(['element'])
            # wait until element is processed by consumer
            consumed_latch.wait(10)
        finally:
            # close the disruptor to verify there is no hangup
            disruptor.close()

        self.assertEqual(['element'], consumer.consumed)
