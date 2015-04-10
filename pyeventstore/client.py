import asyncio
import uuid
import json

import requests
from requests.exceptions import HTTPError

from pyeventstore.events import get_all_events, start_subscription
from pyeventstore.stream_page import StreamPage


class Client:

    def __init__(self, host, secure=False, port=2113):
        proto = "https" if secure else "http"
        self.base_url = proto + "://" + host + ":" + str(port)

    def post_events(self, stream_name, events):
        url = '{}/streams/{}'.format(self.base_url, stream_name)
        headers = {'Content-Type': 'application/vnd.eventstore.events+json'}
        response = requests.post(url, headers=headers, data=json.dumps(events))
        if response.status_code >= 400 and response.status_code < 500:
            raise ValueError(response.reason)

    def publish_event(self, stream_name, event_type, data, event_id=None):
        if event_id is None:
            event_id = str(uuid.uuid4())

        event = {
            'eventId': event_id,
            'eventType': event_type,
            'data': data
        }
        self.post_events(stream_name, [event])

    def stream_head_uri(self, stream_name):
        return '{}/streams/{}'.format(self.base_url, stream_name)

    @asyncio.coroutine
    def get_all_events(self, stream_name):
        head_uri = self.stream_head_uri(stream_name)
        return (yield from get_all_events(head_uri))

    @asyncio.coroutine
    def subscribe_async(self, stream_name, interval_seconds=1):
        head_uri = self.stream_head_uri(stream_name)
        return (yield from start_subscription(head_uri, interval_seconds))

    def get_projection(self, projection_name):
        uri = self.base_url + '/projection/{}'.format(projection_name)
        headers = {'Accept': 'application/json'}
        response = requests.get(uri, headers=headers)
        return response.json()

    def get_projection_state(self, projection_name, partition=None):
        uri = self.base_url + '/projection/{}/state'.format(projection_name)
        headers = {'Accept': 'application/json'}
        params = {}
        if partition:
            params['partition'] = partition
        response = requests.get(uri, headers=headers, params=params)
        return response.json()
