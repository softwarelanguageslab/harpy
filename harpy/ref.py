from harpy._context import currentContext
from harpy._messages import ReactToMsg

from thespian.actors import ActorSystem

class _BaseRef:
    def __init__(self, addr): self.addr = addr

class ActorRef(_BaseRef):
    def send(self, msg): currentContext.ctx.send(self.addr, msg)

class ReactorRef(_BaseRef):
    def react_to(self, ref, source):
        currentContext.ctx.send(self.addr, ReactToMsg(ref, source))

