from dataclasses import dataclass

@dataclass
class InitMsg:
    args: tuple
    kwargs: dict

@dataclass
class EmitMsg:
    value: any

@dataclass
class ReactToMsg:
    ref: any
    source: str

class SubscribeMsg:
    pass

class UnsubscribeMsg:
    pass

