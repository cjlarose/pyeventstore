from enum import Enum


class Position(Enum):
    """
    <- Beginning
    Event 0 <- First
    Event 1
    Event 2
    Event 3
    Event 4
    Event 5 <- Last
    <- Head
    """
    beginning = 0
    first = 1
    last = 2
    head = 3


class StreamReader:
    """
    StreamReader('ponies') # by default, reads from the stream head going
                             forward, starting a subscription
    StreamReader('ponies').startingFrom(12) # starts from event 12
    StreamReader('ponies').startingFrom(12).limit(6) # events 12-17
    StreamReader('ponies').startingFrom(Position.first)
      # all events, subscription
    StreamReader('ponies').until(Position.head) # empty
    StreamReader('ponies').startingFrom(Position.first)
      .until(Position.head) # all events
    """

    def __init__(self, head_uri):
        self.event_queue = None
        self.head_uri = head_uri

        self.beginning = Position.head
        self.end = None  # subscription
        self.max_events = None

    def startingFrom(self, event_number_inclusive):
        copy = self.clone()
        copy.beginning = event_number_inclusive
        return copy

    def until(self, position_exclusive):
        copy = self.clone()
        copy.end = position_exclusive
        return copy

    def limit(self, max_events):
        copy = self.clone()
        copy.max_events = max_events
        return copy

    def forever(self):
        return self.until(None)

    def clone(self):
        copy = StreamReader(self.head_uri)
        copy.beginning = self.beginning
        copy.end = self.end
        copy.max_events = self.max_events
        return copy

    def __repr__(self):
        return "<{}(head_uri='{}', beginning={}, end={}, max_events={})>"\
            .format(self.__class__.__qualname__,
                    self.head_uri,
                    self.beginning,
                    self.end,
                    self.max_events)
