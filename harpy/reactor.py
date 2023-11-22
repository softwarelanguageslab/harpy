import functools
import reactivex

class Reactor:
    @classmethod
    def with_sources(cls, *source_names):
        def build_dag_wrapper(build_dag_fn):
            @functools.wraps(build_dag_fn)
            def create_class_wrapper(*build_dag_args, **build_dag_kwargs):
                def __init__(self):
                    cls.__init__(self, *source_names)
                def build_dag(self, *source_args):
                    args = source_args + build_dag_args
                    return build_dag_fn(*args, **build_dag_kwargs)
                return type(
                    build_dag_fn.__name__,
                    (cls,),
                    {'__init__': __init__, 'build_dag': build_dag}
                )()
            return create_class_wrapper
        return build_dag_wrapper

    def __init__(self, *source_names):
        self._sources = {}
        build_source_args = ()
        for name in source_names:
            subject = reactivex.Subject()
            self._sources[name] = subject
            build_source_args += (subject,)

        observable = self.build_dag(*build_source_args)
        observable.subscribe(lambda value: print("Received {0}".format(value)))
        self._observable = observable
        self._subscribers = []
