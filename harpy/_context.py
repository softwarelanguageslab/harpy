"""Harpy internals. Don't rely on the behaviour of this module.

Thespian makes a distinction between code "inside" the actor system and code
"outside" the system. We wrap around this to have a uniform API regardless of
the context from which a message is sent to an actor.

The `SystemContext` is used "outside" the system, while the `ActorContext` is
used inside an Actor. Harpy's BaseActor and Actor classes must ensure the
`ActorContext` is used when appropriate.

Thespian has multiple backends called "systems". These backends are used to
spawn actors and send them messages. Harpy uses a single of these systems and
wraps it for internal use later. The SystemContext wraps this system.

Note that the multiprocTCPBase system seems to be broken on OSX. As such,
thespian must be used inside docker for now.
"""

from thespian.actors import ActorSystem

class SystemContext:
    _thespianSystem = None

    def __new__(cls):
        if not cls._thespianSystem: cls._thespianSystem = super().__new__(cls)
        return cls._thespianSystem

    def __init__(self):
        # Thespian does not seem to like it if the actorsystem is initialised
        # too early, so we do it on demand (in create).
        self.system = None
        self.thespianSystemBase = 'multiprocTCPBase'

    def overrideSystemBase(self, base):
        assert self.system is None, \
                "system base can only be overriden before starting actors."
        self.thespianSystemBase = base

    def send(self, dst, msg):
        self.system.tell(dst, msg)

    def create(self, cls):
        if not self.system:
            self.system = ActorSystem(systemBase = self.thespianSystemBase)
        return self.system.createActor(cls)

class ActorContext:
    def send(self, dst, msg): self.actor.send(dst, msg)
    def create(self, cls): return self.actor.createActor(cls)
    def wake_up_after(self, time, value): self.actor.wakeupAfter(time, value)

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
