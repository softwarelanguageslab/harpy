from harpy.ref import ReactorRef
from harpy._baseActor import BaseActor

from reactivex import Subject

class Reactor(BaseActor):
    @staticmethod
    def _wrapRef(addr): return ReactorRef(addr)

    def __init__(self):
        super().__init__()
        self._harpy_sources = {}
        for name in self.__class__._harpy_reactor_source_names:
            self._harpy_sources[name] = (Subject(), None)

    def __init_actor__(self, *constructor_args, **constructor_kwargs):
        subjects = (
            self._harpy_sources[name][0] for name in
            self.__class__._harpy_reactor_source_names
        )
        args = tuple(subjects) + constructor_args
        self.observable = self.build_dag(*args, **constructor_kwargs)
        self.observable.subscribe(lambda value: self.emit(value))

    def receiveMsg_ReactToMsg(self, msg, sender):
        (subj, prev) = self._harpy_sources[msg.source]
        if prev: self._send_unsubscribe(prev)
        self._harpy_sources[msg.source] = (subj, msg.ref.addr)
        self._send_subscribe(msg.ref.addr)

    def receiveMsg_EmitMsg(self, msg, sender):
        for (subj, ref) in self._harpy_sources.values():
            if sender == ref: subj.on_next(msg.value)

    def receiveUnrecognizedMessage(self, msg, _sender):
        raise RuntimeError("Reactor {} received unrecognized message: {}".format(self, msg))

# Creating reactors
# -----------------

def sources(*source_names):
    '''
    Reactor subclass decorator

    This decorator can be used to declare the sources a reactor will react to
    when defining a reactor as a `Reactor` subclass. It accepts an arbitary
    amount of strings as its argument. Each of these strings represent the name
    of a single source the reactor will react to. Duplicate strings are not
    allowed.

    As an example, the following class definition:
    ```
    @sources("source1", "source2")
    class MyReactor(Reactor):
        def build_dag(source1, source2, someOtherArgument):
            ...
    ```
    defines a reactor which reacts to two sources: `source1` and `source2`.

    Raises:
        `AssertionError` if the decorated class does not subclass `Reactor`.
    '''
    def reactor_class_wrapper(cls):
        assert issubclass(cls, Reactor), \
               "{} should subclass Reactor".format(cls)
        cls._harpy_reactor_source_names = source_names
        return cls
    return reactor_class_wrapper

def reacts_to(*source_names):
    def reactor_fn_wrapper(reactor_fn):
        def build_dag(self, *args, **kwargs):
            return reactor_fn(*args, **kwargs)

        cls = type(reactor_fn.__name__, (Reactor,), {'build_dag': build_dag})
        cls._harpy_reactor_source_names = source_names
        cls.__module__ = reactor_fn.__module__
        return cls
    return reactor_fn_wrapper
