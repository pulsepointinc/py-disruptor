from threading import RLock, Thread, Condition, Event
import time


class RingBuffer(object):
    """
    Fast, list backed, preallocated Ring Buffer with static size.  Not thread safe and has no safety checking.
    """

    def __init__(self, size):
        """
        Construct a RingBuffer
        :param size: size of buffer
        :type size: int
        """
        self.buffer = []
        self.size = size
        for i in range(self.size):
            self.buffer.append(None)

    def set(self, index, element):
        """
        Set a single element at supplied positive index; index can wrap around size of buffer

        :param index: a positive index - can wrap around size of buffer
        :type index: int
        :param element: element to set
        :type element: *
        :returns: self
        :rtype: RingBuffer
        """
        self.buffer[index % self.size] = element
        return self

    def get(self, index):
        """
        Get a single element at supplied positive index; index can wrap around size of buffer

        :param index: a positive index - can wrap around size of buffer
        :returns: element at index
        :rtype: *
        """
        return self.buffer[index % self.size]

    def mget(self, start_index, count):
        """
        Get multiple elements from buffer

        :param start_index: a positive start index - can wrap around size of buffer
        :type start_index: int
        :param count: number of elements to get - must be >= 0
        :type count: int
        :returns: list of elements
        :rtype: list
        """
        s_index = start_index % self.size
        if s_index + count > self.size:
            return self.buffer[s_index:] + self.buffer[0:(s_index + count) - self.size]
        else:
            return self.buffer[s_index:s_index+count]

    def mset(self, start_index, elements):
        """
        Set multiple elements in buffer

        :param start_index: a positive start index - can wrap around size of buffer
        :type start_index: int
        :param elements: collection of elements
        :type elements: list
        :returns: self
        :rtype: RingBuffer
        """
        s_index = start_index % self.size
        if s_index + len(elements) > self.size:
            self.buffer[s_index:self.size] = elements[0:self.size - s_index]
            self.buffer[0:len(elements) - (self.size - s_index)
                        ] = elements[self.size-s_index:len(elements)]
        else:
            self.buffer[s_index:s_index +
                        len(elements)] = elements[0:len(elements)]


class ConsumerStats(object):
    """
    Statistics object for keeping track of consumer stats
    """

    def __init__(self, consumer_t):
        """
        Construct a consuemr for a consumer thread
        :param consumer_t: consumer thread
        :type consumer_t: ConsumerThread
        """
        self.consumer_t = consumer_t
        self.blocked_sec = 0
        self.consumed = 0
        self.consumption_sec = 0

    def report_blocked(self, sec):
        """
        Report an instance of a consumer being blocked on production for supplied seconds
        :param sec: time consumer was blocked in seconds
        :type sec: float
        """
        self.blocked_sec = self.blocked_sec + sec

    def report_consumed(self, n_elements, sec):
        """
        Report a consumer consuming a number of elements over some time period in seconds
        :param n_elements: number of elements consumed
        :type n_elements: int
        :param sec: time taken to consume elements
        :type sec: float
        """
        self.consumed = self.consumed + n_elements
        self.consumption_sec = self.consumption_sec + sec

    @property
    def cps(self):
        """
        Return elements consumed per second
        :returns: elements consumed per second
        :rtype: float
        """
        if self.consumption_sec > 0:
            return self.consumed / self.consumption_sec
        else:
            return 0

    def __str__(self):
        return '\n'.join([
            'Consumer: {}'.format(self.consumer_t.consumer),
            ' blocked_sec:{}'.format(self.blocked_sec),
            ' consumed:{}'.format(self.consumed),
            ' consume_sec:{}'.format(self.consumption_sec),
            ' cps:{}'.format(self.cps)
        ])


class RingBufferLagStats(object):
    """
    Basic disruptor ring buffer "lag" statistics
    used to keep track of how far the slowest consumer is behind producers
    """

    def __init__(self):
        self.cur_lag = 0
        self.max_lag = 0
        self.avg_lag = 0
        self.n_samples = 0

    def sample(self, lag):
        """
        Supply a lag sample
        :param lag: number of elements slowest consumer is behind producers
        :type lag: int
        """
        self.cur_lag = lag
        if lag > self.max_lag:
            self.max_lag = lag
        self.avg_lag = ((self.avg_lag * self.n_samples) + lag) / \
            (self.n_samples + 1)
        self.n_samples = self.n_samples + 1

    def __str__(self):
        return 'cur_lag: {}, avg_lag: {}, max_lag: {}'.format(self.cur_lag, self.avg_lag, self.max_lag)


class DisruptorStats(object):
    """
    Disruptor statistics container
    """

    def __init__(self, time_fn):
        self.time_fn = time_fn
        self.consumer_stats = {}
        self.p_blocked_sec = 0
        self.produced = 0
        self.ring_lag_stats = RingBufferLagStats()
        self.start_time = time_fn()
        self.end_time = None

    def report_c_consumed(self, consumer_t, n_elements, sec):
        """
        Report a consumer thread consuming some number of elements
        :param consumer_t: a ConsumerThread
        :type consumer_t: ConsumerThread
        :param n_elements: number of elements consumed
        :type n_elements: int
        :param sec: time taken to consume elements
        :type sec: float
        """
        self.consumer_stats.setdefault(consumer_t.thread.name, ConsumerStats(
            consumer_t)).report_consumed(n_elements, sec)

    def report_p_produced(self, n_elements):
        """
        Report a producer producing some elements into the disruptor
        :param n_elements: number of elements produced
        """
        self.produced = self.produced + n_elements

    def report_c_blocked(self, consumer_t, sec):
        """
        Report an instance of a consumer being blocked on production for supplied seconds
        :param consumer_t: a ConsumerThread
        :type consumer_t: ConsumerThread
        :param sec: time consumer was blocked in seconds
        :type sec: float
        """
        self.consumer_stats.setdefault(consumer_t.thread.name, ConsumerStats(
            consumer_t)).report_blocked(sec)

    def report_p_blocked(self, sec):
        """
        Report a publisher being blocked on a "full" ring for supplied number of seconds
        :param sec: time producer was blocked in seconds
        :type sec: float
        """
        self.p_blocked_sec = self.p_blocked_sec + sec

    def report_ring_lag(self, lag_size):
        """
        Report some ring lag
        :param lag_size: number of elements slowest consumer is behind producers
        :type lag_size: int
        """
        self.ring_lag_stats.sample(lag_size)

    def close(self):
        self.end_time = self.time_fn()

    @property
    def production_sec(self):
        """
        Return number of seconds disruptor was operational
        :returns: number of seconds disruptor was operational
        :rtype: float
        """
        if self.end_time != None:
            return self.end_time - self.start_time
        else:
            return self.time_fn() - self.start_time

    @property
    def pps(self):
        """
        Return elements produced per second
        :returns: elements produced per second
        :rtype: float
        """
        if self.production_sec > 0:
            return self.produced / self.production_sec
        else:
            return 0

    def __str__(self):
        return '\n'.join([
            'Ring: {}'.format(self.ring_lag_stats),
            'Producers:',
            ' blocked_sec:{}'.format(self.p_blocked_sec),
            ' produced:{}'.format(self.produced),
            ' produce_sec:{}'.format(self.production_sec),
            ' pps:{}'.format(self.pps),
        ] + [str(value) for (key, value) in self.consumer_stats.items()])


class RingSynchronizer(object):
    """
    Disruptor synchronization uitlity
    """

    def __init__(self):
        """
        Construct RingSynchronizer
        """
        self.lock = RLock()
        self.produced_condition = Condition(self.lock)
        self.consumed_condition = Condition(self.lock)

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, t, v, tb):
        self.lock.release()

    def await_production(self, timeout_sec=5):
        """
        Block calling thread for supplied amount of time or until a production condition occurs

        :param timeout_sec: timeout seconds
        :type timeout_sec: float
        """
        with self.lock:
            self.produced_condition.wait(timeout_sec)

    def await_consumption(self, timeout_sec=5):
        """
        Block calling thread for supplied amount of time or until a consumption condition occurs

        :param timeout_sec: timeout seconds
        :type timeout_sec: float
        """
        with self.lock:
            self.consumed_condition.wait(timeout_sec)

    def notify_production(self):
        """
        Notify and unblock all threads waiting on a production condition
        """
        with self.lock:
            self.produced_condition.notify_all()

    def notify_consumption(self):
        """
        Notify and unblock all threads waiting on a consumption condition
        """
        with self.lock:
            self.consumed_condition.notify_all()


class ConsumerThread(object):
    """
    A disruptor consumption thread wrapper
    """

    def __init__(self, disruptor, consumer):
        self.disruptor = disruptor
        self.consumer = consumer
        self.index = disruptor.producer_index
        self.thread = Thread(
            name='{}_Consumer_{}'.format(disruptor.name, consumer), target=self.run)
        self.thread.start()

    def run(self):
        """
        Consume while there's something to consume!
        """
        available_count = 0
        to_consume = None
        while self.disruptor.running:

            # report ring lag once in a while
            self.disruptor._unsafe_report_lag()

            while available_count == 0 and self.disruptor.running:
                # atomically check+fetch available data and go to sleep if none
                with self.disruptor.sync:
                    available_count = self.disruptor.producer_index - self.index
                    if available_count > 0:
                        to_consume = self.disruptor.ring_buffer.mget(
                            self.index, available_count)
                    else:
                        s = self.disruptor.time_fn()
                        self.disruptor.sync.await_production()
                        self.disruptor.stats.report_c_blocked(
                            self, self.disruptor.time_fn()-s)

            # when data is available, consume it *outside* of lock!
            self._consume_safe(to_consume)

            # re-lock after consumption to update state
            with self.disruptor.sync:
                self.index = self.index + available_count
                self.disruptor.sync.notify_consumption()
                available_count = 0
                to_consume = None

        # after disruptor stops, consume the rest of available data
        to_consume = None
        with self.disruptor.sync:
            available_count = self.disruptor.producer_index - self.index
            if available_count > 0:
                to_consume = self.disruptor.ring_buffer.mget(
                    self.index, available_count)
        self._consume_safe(to_consume)
        self.consumer.close()

    def _consume_safe(self, elements):
        if elements != None and len(elements) > 0:
            s = self.disruptor.time_fn()
            try:
                self.consumer.consume(elements)
            except Exception as e:
                if self.disruptor.consumer_error_handler != None:
                    self.disruptor.consumer_error_handler(
                        self.consumer, elements, e)
            self.disruptor.stats.report_c_consumed(
                self, len(elements), self.disruptor.time_fn() - s)


class Disruptor(object):
    """
    A basic multi-threaded RingBuffer; a Disruptor-Lite implementation using a thread-per-consumer model.

    This is an efficient alternative to queues in cases where multiple workloads need to consume the same type of data.

    See the below links for an explanation of how this works:
        * http://mechanitis.blogspot.com/2011/07/dissecting-disruptor-writing-to-ring.html explanation of structure
        * https://lmax-exchange.github.io/disruptor/ original Disruptor
        * https://medium.com/@teivah/understanding-the-lmax-disruptor-caaaa2721496 - mechanical sympathy (largely not applicable in python)

    Allows for many producers to efficiently concurrently produce elements to be concurrently consumed by many consumers.
    Uses a shared ring buffer between consumers and producers with a minimum amount of blocking.  An element in the ring
    can only be written to after all consumers have consumed it, effectively handling backpressure from any number of 
    parallel consumers.

    Producers (callers of the produce method) are only blocked when the ring is full.

    Unlike a C/C++/C#/Java disruptor, this implementation uses Python threads, so it offers little to no mechanical sympathy
    outside of supporting lock-free parallel consumptions of big sections of the ring buffer which may or may not fall into
    a single CPU cache line.  Even so, this is a great tool for processing multiple events of the same type concurrently.
    """

    def __init__(self, size=1024, name='Disruptor', consumer_error_handler=None, time_fn=time.time):
        """
        Initialize a disruptor of supplied size
        :param size: size of disruptor ring buffer
        :type size: int
        :param name: disruptor name (used in naming consumer threads)
        :type name: str
        :param consumer_error_handler: optional function invoked if a consumer fails to consume data.  Must accept a consumer instance, input to consumer, and error
        :type consumer_error_handler: Function
        :param time_fn: zero argument time provider function - defaults to time.time(); used strictly for statistics
        :type time_fn: Function
        """
        self.name = name
        self.time_fn = time_fn
        self.consumer_error_handler = consumer_error_handler
        self.stats = DisruptorStats(time_fn)
        self.ring_buffer = RingBuffer(size)
        self.sync = RingSynchronizer()
        self.producer_index = 0
        self.consumers = []
        self.running = True

    def produce(self, elements):
        """
        Produce multiple elements, blocking if the disruptor is full

        :param elements: list of elements to produce
        :type elements: list
        """
        if not self.running:
            raise Exception('Disruptor is stopped')

        # report ring lag once in a while
        self._unsafe_report_lag()

        produced = 0

        while produced < len(elements):
            with self.sync.lock:
                # figure out maximum production index
                can_produce = self.ring_buffer.size - \
                    (self.producer_index - min(map(lambda c: c.index, self.consumers)))
                if can_produce <= 0:
                    s = self.time_fn()
                    self.sync.await_consumption()
                    self.stats.report_p_blocked(self.time_fn()-s)
                else:
                    to_produce_cnt = min(can_produce, len(elements) - produced)
                    self.ring_buffer.mset(
                        self.producer_index, elements[produced:produced+to_produce_cnt:])
                    produced += to_produce_cnt
                    self.producer_index += to_produce_cnt
                    self.sync.notify_production()

        self.stats.report_p_produced(produced)

    def close(self):
        """
        Close the disruptor, block (potentially indefinitely) until consumers finish consuming all elements
        """
        with self.sync.lock:
            if self.running:
                self.running = False
                # wake up any threads waiting on production
                self.sync.notify_production()
        for consumer in self.consumers:
            consumer.thread.join()
        self.stats.close()

    def register_consumer(self, consumer):
        """
        Register a consumer for this disruptor.  Once registered, a consumer can not be de-registered!

        :param consumer: a Consumer
        :type consumer: Consumer
        """
        self.consumers.append(ConsumerThread(self, consumer))
        return self

    def _unsafe_report_lag(self):
        if self.producer_index % 10 == 3:
            self.stats.report_ring_lag(
                self.producer_index - min(map(lambda c: c.index, self.consumers)))

    def __str__(self):
        return 'Disruptor-"{}"'.format(self.name)

    def __del__(self):
        self.close()


class Consumer(object):
    """
    A no-op consumer class; can be extended for convenience
    """

    def consume(self, elements):
        """
        Consume elements
        :param elements: a list of elements
        :type elements: list
        """
        raise NotImplementedError()

    def close(self):
        """
        Close consumer, finishing any consumption tasks e.g. terminating batches
        """
        pass

    def __str__(self):
        return self.__class__.__name__


class BatchConsumer(Consumer):
    """
    A Consumer that buffers incoming elements into batches of specified size without using iteration.

    Useful for workloads that are optimized to a specific batch size.
    """

    def __init__(self, batch_size):
        """
        Construct a BatchConsumer with supplied batch size
        :param batch_size: a batch size
        :type batch_size: int
        """
        self.batch_size = batch_size
        self._batch = []

    def consume(self, elements):
        consumed = 0
        while consumed < len(elements):
            to_consume_cnt = min(
                self.batch_size - len(self._batch), len(elements)-consumed)
            self._batch.extend(elements[consumed:consumed+to_consume_cnt])
            if len(self._batch) >= self.batch_size:
                self.consume_batch(self._batch)
                self._batch = []
            consumed += to_consume_cnt

    def close(self):
        if len(self._batch) > 0:
            self.consume_batch(self._batch)

    def consume_batch(self, batch):
        """
        Consume a batch of elements
        :param batch: a batch of elements of batch_size or smaller
        :type batch: list
        """
        raise NotImplementedError()
