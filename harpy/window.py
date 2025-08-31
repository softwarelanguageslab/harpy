"""Window definitions.

Harpy windows are used to perform aggregations on all events that occurred
within a given timespan. Whenever a window receives a value, its `timestamp`
function is called, which has to specify the time associated with the value.
"""

from harpy._baseActor import BaseActor
from harpy.ref import WindowRef

from dataclasses import dataclass
from datetime import timedelta
import time
import math

class Window(BaseActor):
    """Window class.

    A harpy window can be created by extending this class and decorating it
    with a _window assigner_ (such as  `FixedWindow` or `SlidingWindow`). When
    this is done, the subclass must define the `timestamp`, `add_to_window` and
    `window_complete` functions documented below. Additionally, the `key`
    method optionally be implemented.
    """

    @staticmethod
    def _wrapRef(addr): return WindowRef(addr)

    def __init__(self):
        super().__init__()
        self._harpy_subscriptions = []
        self._harpy_window_panes = {}

    def __init_actor__(self): pass

    def timestamp(self, value):
        """Obtain the timestamp of a given value.

        This method must be implemented when defining a window subclass.
        It receives the data record received by the window and must return a
        timestamp which identifies the event or processing time at which the
        data record "occurred".
        """
        raise NotImplementedError()

    def key(self, value):
        """Obtain the key of a given value.

        This is an optional method which can be used to not only partition
        incoming data records based on _when_ they occurred, but also on a key.
        Data records will only be added to the same window if their timestamp
        falls in the same window _and_ if they have the same key.
        """
        return None

    def add_to_window(self, value, window):
        """Add a data element to a window.

        This method must be implemented by every window subclass. It receives
        a data record (`value`) and the current contents of the window. It must
        return a value which represents the new contents of the window. The new
        contents of the window will be passed to `add_to_window` next time a
        data element is received by this particular window.

        When the window is empty, the value of `window` will be `None`.
        """
        raise NotImplementedError()

    def window_complete(self, window):
        """Finalize a window.

        This method must be implemented by every window subclass. It is called
        when the trigger of the window is triggered. Said otherwise, it is
        triggered when the window is "complete". It receives the window and may
        use `emit` to produce some data for the window.
        """
        raise NotImplementedError()

    def receiveMsg_ReactToMsg(self, msg, sender):
        self._harpy_subscriptions.append((msg.ref.addr, msg.stream))
        self._send_subscribe(msg.ref.addr, msg.stream)

    def receiveMsg_EmitMsg(self, msg, sender):
        key = self.key(msg.value)
        timestamp = self.timestamp(msg.value)
        windows = self._harpy_window_assigner.windows_for(timestamp, msg.value)

        # Add objects to the window
        for window in windows:
            pane = self._harpy_window_panes.get((window, key))
            updated = self.add_to_window(msg.value, pane)
            self._harpy_window_panes[(window, key)] = updated

        # Trigger elapsed windows
        # TODO: improve trigger logic later
        for ((start, end), key) in list(self._harpy_window_panes.keys()):
            if timestamp > end:
                pane = self._harpy_window_panes[((start, end), key)]
                self.window_complete(pane)
                del self._harpy_window_panes[((start, end), key)]

    def receiveUnrecognizedMessage(self, msg, _sender):
        raise RuntimeError(
            "Window {} received unrecognized message: {}".format(self, msg)
        )

# ---------------- #
# Window Assigners #
# ---------------- #

class _WindowAssigner:
    def __call__(self, cls):
        assert issubclass(cls, Window), "{} should subclass Window".format(cls)
        cls._harpy_window_assigner = self
        return cls

class FixedWindow(_WindowAssigner):
    """Window assigner which creates non-overlapping windows of a fixed size.

    This windowassigner should be used as a decorator decorating a class which
    subclasses Window. When this is done, the class will assign assign incoming
    data elements to fixed windows.

    Fixed windows are windows of a fixed size which do not overlap. When one
    window ends, the next one begins. The window is specified by specifying a
    length and an optional offset. Both of these arguments are specified as a
    python timedelta. The length specifies how large the window should be.
    For instance, `@FixedWindow(timedelta(hours=1))` would create a new window
    every hour. By default, windows are "aligned" to the unix epoch. This means
    that windows of the previous example would always start at the beginning
    of the hour. To change this, the `offset` argument can be used to specify
    an offset relative to the epoch. As an example,
    `@FixedWindow(timedelta(hours=1), timedelta(minutes=-5))` specifies a
    window of an hour long, which always starts 5 minutes before the hour
    elapses.
    """
    def __init__(self, length, offset=timedelta(seconds=0)):
        self.length = length.total_seconds()
        self.offset = offset.total_seconds()

    def windows_for(self, timestamp, _value):
        start = timestamp - ((timestamp - self.offset) % self.length)
        end = start + self.length
        return [(start, end)]

class SlidingWindow(_WindowAssigner):
    """Window assigner which creates windows with a fixed size and frequency.

    This windowassigner should be used as a decorator decorating a class which
    subclasses Window. When this is done, the class will assign assign incoming
    data elements to sliding windows.

    Sliding windows are windows which occur at a fixed _frequency_ (e.g. every
    hour) with a fixed length (e.g. 100 minutes). Sliding windows may overlap
    (when the length is larger than the frequency) or may not cover all
    possible data elements (when the length is smaller than the frequency).
    Fixed windows can be used instead when the length and frequency are equal
    to each other.

    Like fixed windows, sliding windows are aligned to the epoch. An offset may
    be provided to change the alignment.
    """
    def __init__(self, frequency, length, offset=timedelta(seconds=0)):
        self.frequency = frequency.total_seconds()
        self.length = length.total_seconds()
        self.offset = offset.total_seconds()

    def windows_for(self, timestamp, _value):
        last_start = timestamp - ((timestamp - self.offset) % self.frequency)

        window = last_start
        windows = []

        while window > timestamp - self.length:
            windows.append((window, window + self.length))
            window = window - self.frequency

        return windows
