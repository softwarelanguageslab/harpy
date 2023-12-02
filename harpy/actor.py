from harpy.ref import ActorRef
from harpy._baseActor import BaseActor

class Actor(BaseActor):
    @staticmethod
    def _wrapRef(addr): return ActorRef(addr)

    def __init__(self):
        super().__init__()
        self._harpy_monitoring = []

    def __init_actor__(self):
        pass

    def receiveUnrecognizedMessage(self, msg, sender):
        with self._harpy_context as context:
            self.receive(msg)

    def monitor(self, ref, message_name):
        method = getattr(self, message_name)
        self._harpy_monitoring.append((ref.addr, method))
        self._send_subscribe(ref.addr)

    def unmonitor(self, ref):
        method = self._find_method(ref.addr)
        self._harpy_monitoring.remove((ref.addr, method))
        self._send_unsubscribe(ref.addr)

    def receiveMsg_EmitMsg(self, msg, sender):
        method = self._find_method(sender)
        with self._harpy_context as context:
            if method: method(msg)

    def _find_method(self, sender):
        for (observing, method) in self._harpy_monitoring:
            if observing == sender: return method

# TODO: reactor-style decorators
