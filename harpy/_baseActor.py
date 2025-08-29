from collections import defaultdict

from harpy._context import ActorContext, currentContext
from harpy._messages import InitMsg, EmitMsg, SubscribeMsg, UnsubscribeMsg

from thespian.actors import ActorTypeDispatcher

class BaseActor(ActorTypeDispatcher):
    """Harpy's fundamental base actor.

    This class captures all the behaviour shared by actors, reactors and
    windows. It wraps a thespian actor class and handles most of the general
    harpy-level messages defined in the _messages module.

    This class is internal and should not be used directly.
    """
    @classmethod
    def spawn(cls, *args, **kwargs):
        ref = currentContext.ctx.create(cls)
        currentContext.ctx.send(ref, InitMsg(args, kwargs))
        return cls._wrapRef(ref)

    def __init__(self):
        self._harpy_subscribers = defaultdict(list)
        self._harpy_init_pending = True

    def receiveMsg_InitMsg(self, msg, _sender):
        if hasattr(self, "_harpy_init_pending"):
            delattr(self, "_harpy_init_pending")
            self.ref = self._wrapRef(self.myAddress)
            self._harpy_context = ActorContext(self)
            with self._harpy_context as context:
                self.__init_actor__(*msg.args, **msg.kwargs)
        else:
            raise RuntimeError(
                "Actor {} received multiple init messages".format(self)
            )

    def receiveMsg_SubscribeMsg(self, msg, sender):
        self._harpy_subscribers[msg.stream].append(sender)

    def receiveMsg_UnsubscribeMsg(self, msg, sender):
        self._harpy_subscribers[msg.stream].remove(sender)

    def _send_subscribe(self, addr, stream):
        self._harpy_context.send(addr, SubscribeMsg(stream))

    def _send_unsubscribe(self, addr, stream):
        self._harpy_context.send(addr, UnsubscribeMsg(stream))

    def emit(self, val, stream = "default"):
        for subscriber in self._harpy_subscribers[stream]:
            self._harpy_context.send(subscriber, EmitMsg(val, stream))

