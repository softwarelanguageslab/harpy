from harpy._context import ActorContext, currentContext
from harpy._messages import InitMsg, EmitMsg, SubscribeMsg, UnsubscribeMsg

from thespian.actors import ActorTypeDispatcher

class BaseActor(ActorTypeDispatcher):
    @classmethod
    def spawn(cls, *args, **kwargs):
        ref = currentContext.ctx.create(cls)
        currentContext.ctx.send(ref, InitMsg(args, kwargs))
        return cls._wrapRef(ref)

    def __init__(self):
        self._harpy_subscribers = []
        self._harpy_init_pending = True

    def receiveMsg_InitMsg(self, msg, _sender):
        if hasattr(self, "_harpy_init_pending"):
            delattr(self, "_harpy_init_pending")
            self.ref = self._wrapRef(self.myAddress)
            self._harpy_context = ActorContext(self)
            with self._harpy_context as context:
                self.__init_actor__(*msg.args, **msg.kwargs)
        else:
            raise RuntimeError("Actor {} received multiple init messages".format(self))

    def receiveMsg_SubscribeMsg(self, msg, sender):
        self._harpy_subscribers.append(sender)

    def receiveMsg_UnsubscribeMsg(self, msg, sender):
        self._harpy_subscribers.remove(sender)

    def emit(self, val):
        for subscriber in self._harpy_subscribers:
            self._harpy_context.send(subscriber, EmitMsg(val))

    def _send_subscribe(self, addr):
        self._harpy_context.send(addr, SubscribeMsg())

    def _send_unsubscribe(self, addr):
        self._harpy_context.send(addr, UnsubscribeMsg())
