from reactivex import Subject
from thespian.actors import ActorTypeDispatcher

class InitMsg:
    def __init__(self, args, kwargs):
        self.args = args
        self.kwargs = kwargs

class EmitMsg:
    def __init__(self, source, value):
        self.source = source
        self.value = value

class SubscribeMsg:
    def __init__(self, topic):
        self.topic = topic

class Reactor(ActorTypeDispatcher):
    def __init__(self):
        self._sources = {}
        self._subscribers = []
        self._init_complete = False
        for name in self.__class__._source_names:
            self._sources[name] = Subject()

    def __init_actor__(self, constructor_args, constructor_kwargs):
        subjects = (self._sources[name] for name in self.__class__._source_names)
        args = tuple(subjects) + constructor_args
        self.observable = self.build_dag(*args, **constructor_kwargs)
        self.observable.subscribe(lambda value: self._publish(value))

    # TODO: need actors to test this
    def _publish(self, value):
        for (recv, topic) in self._subscribers:
            self.send(recv, EmitMsg(topic, value))

    def receiveMsg_InitMsg(self, msg, _sender):
        if self._init_complete:
            raise RuntimeError("Reactor {} received multiple init messages".format(self))
        self.__init_actor__(msg.args, msg.kwargs)

    def receiveMsg_EmitMsg(self, msg, _sender):
        self._sources[msg.source].on_next(msg.value)

    def receiveMsg_SubscribeMsg(self, msg, sender):
        self._subscribers += (sender, msg.msg_or_source)

    def receiveUnrecognizedMessage(self, msg, _sender):
        raise RuntimeError("Reactor {} received unrecognized message: {}".format(self, msg))


# Creating reactors
# -----------------

def sources(*source_names):
    '''
    Reactor sublass decorator

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
        cls._source_names = source_names
        return cls
    return reactor_class_wrapper

def reacts_to(*source_names):
    def reactor_fn_wrapper(reactor_fn):
        def build_dag(self, *args, **kwargs):
            return reactor_fn(*args, **kwargs)

        cls = type(reactor_fn.__name__, (Reactor,), {'build_dag': build_dag})
        cls._source_names = source_names
        cls.__module__ = reactor_fn.__module__
        return cls
    return reactor_fn_wrapper

# Temp stuff

def spawn_reactor(beh, *args, **kwargs):
    obj = beh()
    obj.__init_actor__(args, kwargs)
    return obj
