# py-disruptor

A **basic**, **unoptimized** multi-threaded RingBuffer - a "[Disruptor](https://lmax-exchange.github.io/disruptor/)-Lite" implementation using a thread-per-consumer model with python threads.

Works with both python2 and python3.

Generally useful for parallelizable io-bound tasks, such as updating a database.

As described [here](http://mechanitis.blogspot.com/2011/07/dissecting-disruptor-writing-to-ring.html), at its core, a Disruptor is:

* A ring buffer into which producers can write elements to while consumers can consume elements from.
    * The ring has a finite size
    * The elements of the ring are pre-allocated
    * Every element placed into the ring has a sequence number
* There can be many parallel producers:
    * When a producer needs to write an element into the ring
        * the disruptor picks the next available slot in the ring and writes the element there, incrementing the slot's sequence number
        * a slot is considered "Available" only after it has been consumed by all consumers in the disruptor
        * If there are no available slots, the producer is blocked until such time that there is a slot available
* There can be many parallel consumers:
    * The disruptor creates a thread per consumer
        * Each consumer keeps track of the last sequence number it consumed
        * While there is data to be consumed, the consumer consumes data
        * When there is no data to be consumed (the consumer's sequence number is the highest sequence number in the ring), the consumer thread is blocked until there is data available

This allows for an efficient setup for cases where an individual element needs to be consumed by several consumers at a maximum throughput.

A more traditional approach of accomplishing concurrent consumption would be to have a set of parallel consumers simply consume batches of elements, with the producer waiting for each batch to be consumed by each consumer.  This works OK when all consumers work at about the same speed, but is inefficient when some consumers/producers work slower than others.  

With a shared, finite ring buffer, these inefficiencies are largely eliminated.  The throghput of data through the disruptor is still limited by the slowest consumer or producer.  Unlike other approaches though, the structure guarantees that while there can be any work done (elements produced or consumed) it *is* being done - rather than various producer or consumer processes sitting idle.  Effectively, a disruptor handles backpressure [really well](https://github.com/LMAX-Exchange/disruptor/wiki/Performance-Results) - especially in a "multicast" (e.g. `1P->3C`) configuration.

Unlike [Java](https://github.com/LMAX-Exchange/disruptor), [.NET](https://github.com/disruptor-net/Disruptor-net), [C++](https://github.com/Abc-Arbitrage/Disruptor-cpp), and [C](https://github.com/systemtrader/c.LMAX-.disruptor) implementations of disruptors, this version does little in the way of "mechanical sympathy" through loading CPU cache lines.  As a side-effect of the gil / python threads, mechanical sympathy is limited to being able to consume elements in batches.  It's also generally unoptimized and produces a ton of garbage. On the other hand, this version does support:

1. producing/consuming big chunks of data at a time.
2. processing elements outside of any synchronization

## Usage

### Installing py-disruptor
* via `requirements.txt`:
    ```
    ...
    package==version
    ...
    -e https://github.com/pulsepointinc/py-disruptor.git@0.0.1#egg=py-disruptor==0.0.1
    pip install git+https://github.com/myuser/foo.git@v123
    ...
    ```
* via `pip` CLI:
    ```
    pip install https://github.com/pulsepointinc/py-disruptor.git#egg=py-disruptor==0.0.1
    ```
### Running a disruptor

```python
from disruptor import Disruptor, Consumer
import time, random

class MyConsumer(Consumer):
    def __init__(self, name):
        self.name = name
    def consume(self, elements):
        # simulate some random processing delay
        time.sleep(random.random())
        print("{} consumed {}".format(self.name,elements))

# Construct a couple of consumer instances
consumer_one = MyConsumer(name = 'consumer one')
consumer_two = MyConsumer(name = 'consumer two')

# Construct a disruptor named example
disruptor = Disruptor(name = 'Example', size = 3)
try:
    # Register consumers
    disruptor.register_consumer(consumer_one)
    disruptor.register_consumer(consumer_two)

    for i in range(10):
        # Produce a bunch of elements
        element = 'element {}'.format(i)
        disruptor.produce([element])
        print("produced {}".format(element))
finally:
    # Shut down the disruptor
    disruptor.close()
```

See [TestDisruptor](tests/test_disruptor.py#L137) for more examples

### Accessing disruptor statistics

The `Disruptor` keeps track of producer and consumer statistics, including blocked time.  They can be accessed via the `disruptor.stats` object

### Hints

* Implement the `close` method inside consumers that require any kind of cleanup!
* Make sure consumers don't hang forever.  There are no timeout checks for consumption, so a hanging consumer means a hanging disruptor!

## Developing

### Testing
To run unit tests, execute:
* `python -m unittest tests`

### Releasing

This is a simple project released entirely via github.  To release, simply:

1. update `version` in [setup.py](setup.py)
2. run `git tag <new_version>`
3. update `README` usage as appropriate to point to the git tag

