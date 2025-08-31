# Copyright 2025, Mathijs Saey, Vrije Universiteit Brussel

# This file is part of Harpy.
#
# Harpy is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# Harpy is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# harpy. If not, see <https://www.gnu.org/licenses/>.

"""Reactor definitions.

Harpy reactors are used to write event-based code. Instead of defining a novel
embedded language, harpy uses [reactiveX](https://rxpy.readthedocs.io/en)
(version 4) for this purpose.

# Observables, operators and observers

Reactivex introduces the notion of _observables_ which represent sources of
events. Observables can be fed to various _operators_ (such as map, filter,
...), to create new observables. In the end, an observable is consumed by a
_sink_, called an _observer_ in reactivex.

As an example, the reactivex docs provides the following
[code](https://rxpy.readthedocs.io/en/latest/get_started.html#operators-and-chaining):

```
from reactivex import of, operators as op

source = of("Alpha", "Beta", "Gamma", "Delta", "Epsilon")

composed = source.pipe(
    op.map(lambda s: len(s)),
    op.filter(lambda i: i >= 5)
)
composed.subscribe(lambda value: print("Received {0}".format(value)))
```

In this code, `source` defines an initial observable, the events of which are
then processed using the `map` and `filter` operators. Finally, the resulting
values are processed by the `subscribe` function, which creates an observer
which consumes the resulting events, printing them.

# Reactors

Harpy reactors handle the creation of the initial source observable and the
final sink observer for the programmer. Therefore, a programmer writing a harpy
reactor is only responsible for creating a data processing pipeline through the
use of the various operators defined by reactivex.

As an example, the above example would be written as follows as a harpy reactor:

```
from harpy.reactor import reacts_to
from reactivex import operators as op

@reacts_to("words")
def pipeline(words):
    words.pipe(
        op.map(lambda s: len(s)),
        op.filter(lambda i: i >= 5)
    )
```

This code creates a reactor class called `pipeline`. This reactor can be
spawned (`pipeline.spawn()`), after which the spawned reactor can be connected
to other actors, reactors or windows. Whenever an upstream actor, reactor or
window emits data, the pipeline defined above will automatically be invoked
on the received data; the result of the pipeline will then be emitted, to be
sent to any downstream actors, reactors or windows.

As such, harpy reactors can be used to build reactivex data processing
pipelines which are automatically invoked when needed. The `@reacts_to`
annotation is the main mechanism offered by harpy for the creation of reactors,
with the `@lift` decorator serving as a shorthand for creating simple reactors.
"""

from harpy.ref import ReactorRef
from harpy._baseActor import BaseActor

from reactivex import Subject

class Reactor(BaseActor):
    """Reactor abstract base class.

    This class defines the internals of reactors. It should not be instantiated
    directly. Instead, reactors can be created by decorating a function which
    builds a reactivex observable chain with the `@reacts_to` decorator.
    """
    @staticmethod
    def _wrapRef(addr): return ReactorRef(addr)

    def __init__(self):
        super().__init__()
        self._harpy_sources = {}
        self._harpy_subscriptions = []
        for name in self.__class__._harpy_reactor_source_names:
            self._harpy_sources[name] = Subject()

    def __init_actor__(self, *constructor_args, **constructor_kwargs):
        subjects = (
            self._harpy_sources[name] for name in
            self.__class__._harpy_reactor_source_names
        )
        args = tuple(subjects) + constructor_args
        dag = self.build_dag(*args, **constructor_kwargs)
        if isinstance(dag, dict):
            for stream, observable in dag.items():
                # Workaround to ensure all subscriptions don't point to the
                # last value of `stream`.
                observable.subscribe(lambda value, s=stream: self.emit(value, s))
        else:
            dag.subscribe(lambda value: self.emit(value))

    def receiveMsg_ReactToMsg(self, msg, sender):
        subj = self._harpy_sources[msg.source]
        self._harpy_subscriptions.append((msg.ref.addr, msg.stream, subj))
        self._send_subscribe(msg.ref.addr, msg.stream)

    def receiveMsg_EmitMsg(self, msg, sender):
        for (ref, stream, subj) in self._harpy_subscriptions:
            if sender == ref and stream == msg.stream:
                subj.on_next(msg.value)

    def receiveUnrecognizedMessage(self, msg, _sender):
        raise RuntimeError("Reactor {} received unrecognized message: {}".format(self, msg))

# Creating reactors
# -----------------

def reacts_to(*source_names):
    """Reactor creation decorator.

    This decorator can be used to create a reactor by decorating a function.
    The decorator accepts an arbitrary amount of strings as arguments. Each
    string represents the name of a single source the reactor will react to.
    Duplicate strings are not allowed.

    The function that is being decorated should accept a single argument for
    each source specified in the decorator. When the reactor is spawned, the
    decorated function will be called. A reactivex observable will be passed for
    each source defined in the decorator. These observables can then be used
    to build a reactivex observable chain. Thus, the return value of this
    function is expected to be a reactivex observable (but see later).

    For example:

    ```
    from harpy.reactor import reacts_to
    from reactivex import operators as op

    @reacts_to("source")
    def example(source):
        source.pipe(op.map(lambda x: x * 2))
    ```

    defines a reactor which will multiply each value it receives by 2.

    Multiple sources can be passed:

    ```
    @reacts_to("source1", "source2")
    def example(source1, source2):
        source1.pipe(op.zip(source2))
    ```

    The function being decorated can accept additional arguments which do not
    correspond to a source, values for these arguments can be provided when
    spwaning the reactor:

    ```
    @reacts_to("source")
    def example(source, multiplier):
        source.pipe(op.map(lambda x: x * multiplier))

    example.spawn(2)
    ```

    or

    ```
    @reacts_to("source")
    def example(source, multiplier = 2):
        source.pipe(op.map(lambda x: x * multiplier))

    example.spawn(multiplier = 4)
    ```

    By default, the decorated function is expected to return a reactivex
    observable. Any value produced by this observable will be emitted to the
    "default" stream of the reactor. As an alternative, the function may return
    a dictionary which maps stream names to observables. When this is the case,
    the value of each observable will be emitted to the specified stream.

    ```
    @reacts_to("value")
    def example(val, lMultiplier, rMultiplier):
        return {
            'left': val.pipe(rxops.map(lambda v: v * lMultiplier)),
            'right': val.pipe(rxops.map(lambda v: v * rMultiplier))
        }

    example.spawn(2, 3)
    ```

    will emit `2` on stream "left" and `3` on stream "right" if it receives
    value `1`.
    """
    def reactor_fn_wrapper(reactor_fn):
        def build_dag(self, *args, **kwargs):
            return reactor_fn(*args, **kwargs)

        cls = type(reactor_fn.__name__, (Reactor,), {'build_dag': build_dag})
        cls._harpy_reactor_source_names = source_names
        cls.__module__ = reactor_fn.__module__
        return cls
    return reactor_fn_wrapper

def lift(op, source_name = "default"):
    """Lift functions to become reactors.

    This decorator "lifts" a function with a reactivex operator, creating a
    reactor. The created reactor will execute the provided reactivex operator,
    parametrised with the decorated function, on any received value.

    This decorator can only be used to create reactors that listen to a single
    source. It acts as syntactic sugar for creating reactors that execute a
    single function on the values they receive.

    As an example, the following two reactor defintions are equivalent:

    ```
    from reactivex import operators as op
    from harpy.reactor import lift

    @lift(op.map, "value")
    def example(value):
        value * 2
    ```

    ```
    from reactivex import operators as op
    from harpy.reactor import reacts_to

    @reacts_to("value")
    def example(value):
        value.pipe(op.map(lambda x: x * 2))
    ```
    """
    def reactor_fn_wrapper(reactor_fn):
        def build_dag(self, source, *args, **kwargs):
            return source.pipe(op(reactor_fn))

        cls = type(reactor_fn.__name__, (Reactor,), {'build_dag': build_dag})
        cls._harpy_reactor_source_names = [source_name]
        cls.__module__ = reactor_fn.__module__
        return cls
    return reactor_fn_wrapper
