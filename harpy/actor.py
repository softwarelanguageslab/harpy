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

"""Actor definitions.

Harpy actors are used to write "active" code. An actor can perform any
computation and controls its own thread of execution. This is different from
reactors or windows, which can only "react" to incoming data elements and are
not supposed to launch long-lasting computations (e.g. unbounded loops or I/O).

Actors can communicate with each other through the use of asynchronous messages.
Messages received by an actor are processed one by one. If the actor is busy
when it receives a message, the message will be stored inside a message queue
and processed once the actor is ready.

Programmers can use actor references to send data to other actors. In the
actor-reactor model, however, reactors and windows are typically connected to
each other (and to actors) through the use of streams. To bridge this gap, harpy
actors can _emit_ data on an output stream, which will cause the produced data
to be processed by any reactors or windows connected to the output stream.
Additionally, actors can monitor a stream produced by an actor, reactor, or
window in which case any message emitted on this stream will be delivered to
the actor as a message.

Actors are usually created by subclassing Harpy's `Actor` class. However, Harpy
defines several decorators which simplify the creation of such a class for
various common actor "types".
"""

from harpy.ref import ActorRef
from harpy._baseActor import BaseActor

class Actor(BaseActor):
    """Actor class.

    A harpy actor can be created by extending this class. When this is done,
    the following methods may be provided:

    - `__init_actor__(self, *args, **kwargs)`: initializes the actor. Any
    arguments passed to `<classname>.spawn()` will be passed to this method.
    Note that you should **not** implement `__init__`, as this will break
    harpy. Implementing this method is optional.

    - `receive(self, msg)`: called by harpy whenever the actor receives a
    message. An error will be raised if an actor that does not implement this
    method receives a message.
    """

    @staticmethod
    def _wrapRef(addr): return ActorRef(addr)

    def __init__(self):
        super().__init__()
        self._harpy_monitoring = []

    def __init_actor__(self):
        pass

    def receiveUnrecognizedMessage(self, msg, sender):
        self._harpy_dirty_internal_trick = sender
        with self._harpy_context as context:
            self.receive(msg)

    def thisActor(self):
        """Obtain a reference to the current actor instance."""
        return self._wrapRef(self.myAddress)

    def send_self_after(self, time, msg = None):
        """Send a message to the current actor instance after a timeout.

        `time` must be provided as a `datetime.timedelta` object.
        """
        self._harpy_context.wake_up_after(time, msg)

    def receiveMsg_WakeupMessage(self, msg, sender):
        with self._harpy_context as context:
            self.receive(msg.payload)

    def monitor(self, ref, method, stream = "default"):
        """Makes the actor monitor a stream of an actor, reactor or window.

        This method accepts a reference to an actor, reactor or window, a
        stream name, and a method. Whenever an event is emitted on the
        specified stream of the actor, reactor, or window, the provided method
        will be called.
        """
        self._harpy_monitoring.append((ref.addr, stream, method))
        self._send_subscribe(ref.addr, stream)

    def unmonitor(self, ref, stream = "default"):
        """Unmonitor the specified stream of an actor, reactor or window."""
        method = self._find_method(ref.addr, stream)
        self._harpy_monitoring.remove((ref.addr, stream, method))
        self._send_unsubscribe(ref.addr, stream)

    def receiveMsg_EmitMsg(self, msg, sender):
        method = self._find_method(sender, msg.stream)
        with self._harpy_context as context:
            if method: method(msg.value)

    def _find_method(self, sender, stream):
        for (observing, stream, method) in self._harpy_monitoring:
            if observing == sender and stream == stream: return method

def monitor(receive_fn):
    """Create an actor from a function.

    This decorator creates an actor. When the actor is spawned, it accepts a
    reference and, optionally, a stream name. The spawned actor will monitor
    the provided stream and call the decorated function any time a value is
    emitted on the stream.
    """
    def __init_actor__(self, ref, stream = "default"):
        self.monitor(ref, receive_fn, stream)

    methods = {'__init_actor__': __init_actor__}
    cls = type(receive_fn.__name__, (Actor,), methods)
    cls.__module__ = receive_fn.__module__
    return cls

def loop(time):
    """Create a "looping" actor from a class.

    This decorator must decorate a class which subclasses the `Actor` class.
    The decorated class **must** implement a `tick(self)` method, which will be
    called in a loop. The decorated class may implement the `__init_actor__`
    method, which behaves as usual.

    This decorator accepts a `datetime.timedelta` argument, which specifies how
    much time elapses between each call to `tick`. Note that time spent inside
    the `tick` method counts as elapsed time before receiving the next message.
    """
    def actor_class_wrapper(cls):
        init = cls.__init_actor__
        tick = cls.tick

        def wrapped_init(self, *args, **kwargs):
            init(self, *args, **kwargs)
            self.send_self_after(time)

        def receive(self, value):
            self.send_self_after(time)
            tick(self)

        assert issubclass(cls, Actor), "{} should subclass Actor".format(cls)
        cls.__init_actor__ = wrapped_init
        cls.receive = receive
        return cls

    return actor_class_wrapper

