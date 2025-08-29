"""Harpy internal messages.

These messages are system-level messages used by the harpy runtime.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class InitMsg:
    """Message sent by BaseActor to call __init_actor__"""
    args: tuple
    kwargs: dict

@dataclass
class EmitMsg:
    """Message sent when an upstream actor emits a value on a stream"""
    value: Any
    stream: str

@dataclass
class ReactToMsg:
    """Message sent when a reactor or window should subscribe to a stream"""
    ref: Any
    source: str
    stream: str

@dataclass
class SubscribeMsg:
    """Message sent to signify another actor is subscribed to a stream"""
    stream: str

@dataclass
class UnsubscribeMsg:
    """Message sent to signify another actor unsubscribed from a stream"""
    stream: str
