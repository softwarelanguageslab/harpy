"""References to Harpy actors, reactors and windows.

When an actor, reactor or window is spawned, harpy returns a reference to the
spawned instance of the actor, reactor or window. These references can be used
to interact with the spawned actor, reactor or window.
"""

from harpy._context import currentContext
from harpy._messages import ReactToMsg

from thespian.actors import ActorSystem

class _BaseRef:
    def __init__(self, addr): self.addr = addr

class ActorRef(_BaseRef):
    """Reference to a harpy actor."""
    def send(self, msg):
        """Send `msg` to an actor."""
        currentContext.ctx.send(self.addr, msg)

class ReactorRef(_BaseRef):
    """Reference to a harpy reactor."""
    def react_to(self, ref, source = "default", stream = "default"):
        """Tell a reactor to react to a stream of `ref`.

        `ref` can be a reference to an actor, window or another reactor.
        `source` must be the name of a source of the reactor. Whenever the
        actor, reactor or window emits a value on `stream`, the observable for
        the source will be updated with said value.
        """
        currentContext.ctx.send(self.addr, ReactToMsg(ref, source, stream))

class WindowRef(_BaseRef):
    """Reference to a harpy window."""
    def react_to(self, ref, stream = "default"):
        """Tell a window to react to a stream of `ref`."""
        currentContext.ctx.send(self.addr, ReactToMsg(ref, None, stream))
