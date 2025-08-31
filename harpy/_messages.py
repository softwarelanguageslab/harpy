# Copyright 2025, Mathijs Saey, Vrije Universiteit Brussel

# This file is part of Harpy.
#
# Harpy is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# Harpy is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# harpy. If not, see <https://www.gnu.org/licenses/>.

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
