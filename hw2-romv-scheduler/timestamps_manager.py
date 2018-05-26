from collections import namedtuple

# We could create our own type. In real life case, we should create our own type so it would deal with overflows.
Timestamp = int
Timespan = namedtuple('Timespan', ['from_ts', 'to_ts'])


class TimestampsManager:
    def __init__(self):
        self._last_timestamp = Timestamp()

    def get_next_ts(self):
        self._last_timestamp += 1
        return self._last_timestamp

    def peek_next_ts(self):
        return self._last_timestamp + 1
