import harpy._system

class SystemContext:
    def __init__(self): self.system = harpy._system.system
    def send(self, dst, msg): self.system.tell(dst, msg)
    def create(self, cls): return self.system.createActor(cls)

class ActorContext:
    def send(self, dst, msg): self.actor.send(dst, msg)
    def create(self, cls): return self.actor.createActor(cls)

    def __init__(self, actor):
        self.actor = actor
        self.prev = None

    def __enter__(self):
        self.prev = currentContext.ctx
        currentContext.set(self)
        return self

    def __exit__(self, excType, excVal, excStack):
        currentContext.set(self.prev)
        self.prev = None
        return False

class ContextWrapper:
    def __init__(self):
        self.ctx = SystemContext()

    def set(self, ctx):
        self.ctx = ctx

currentContext = ContextWrapper()
