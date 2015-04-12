import asyncio

from pyeventstore.events import (get_all_events,
                                 start_subscription,
                                 publish_events)
from pyeventstore.stream_reader import StreamReader


class Client:

    def __init__(self, host, secure=False, port=2113):
        proto = "https" if secure else "http"
        self.uri_base = '{}://{}:{}'.format(proto, host, port)

    @asyncio.coroutine
    def publish_events(self, stream_name, events):
        uri = self._stream_head_uri(stream_name)
        yield from publish_events(uri, events)

    def _stream_head_uri(self, stream_name):
        return '{}/streams/{}'.format(self.uri_base, stream_name)

    @asyncio.coroutine
    def get_all_events(self, stream_name):
        head_uri = self._stream_head_uri(stream_name)
        return (yield from get_all_events(head_uri))

    @asyncio.coroutine
    def subscribe(self, stream_name, interval_seconds=1):
        head_uri = self._stream_head_uri(stream_name)
        return (yield from start_subscription(head_uri, interval_seconds))

    def read_events(self, stream_name):
        return StreamReader(self._stream_head_uri(stream_name))
