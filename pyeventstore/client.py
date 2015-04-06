import asyncio
import time
import uuid
import json

import requests
from requests.exceptions import HTTPError

from pyeventstore.events import get_all_events_async
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
    def get_all_events_async(self, stream_name, on_event):
        head_uri = self.stream_head_uri(stream_name)
        yield from get_all_events_async(head_uri, on_event)

    def get_stream_page(self, uri):
        headers = {'Accept': 'application/vnd.eventstore.events+json'}
        response = requests.get(uri, headers=headers)
        response.raise_for_status()
        content = response.json()
        return StreamPage(content)

    def get_stream_head(self, stream_name):
        uri = self.base_url + '/streams/' + stream_name
        return self.get_stream_page(uri)

    def fetch_events(self, stream_entries):
        for entry in stream_entries:
            headers = {'Accept': 'application/json'}
            response = requests.get(entry.links['alternate'], headers=headers)
            content = response.json()
            yield entry.summary, content

    def subscribe(self, stream_name, interval_seconds=1):
        last = None
        while last is None:
            try:
                last = self.get_stream_head(stream_name).links['previous']
            except HTTPError as e:
                if e.response.status_code == 404:
                    time.sleep(interval_seconds)
                else:
                    raise e

        while True:
            page = self.get_stream_page(last)
            yield from self.fetch_events(page.entries())

            current = page.links.get('previous', last)
            if last == current:
                time.sleep(interval_seconds)
            last = current

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
