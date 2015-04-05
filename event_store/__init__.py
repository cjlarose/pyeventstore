import json
import uuid
import time

import requests

def links_as_dict(links):
    result = {}
    for link in links:
        result[link['relation']] = link['uri']
    return result

class StreamEntry:
    def __init__(self, content):
        self._content = content
        self._links = None
        self.summary = content['summary']

    @property
    def links(self):
        if self._links is None:
            self._links = links_as_dict(self._content['links'])
        return self._links

class StreamPage:
    def __init__(self, content):
        self._content = content 
        self._links = None

    @property
    def links(self):
        if self._links is None:
            self._links = links_as_dict(self._content['links'])
        return self._links

    def entries(self):
        for entry_content in reversed(self._content['entries']):
            yield StreamEntry(entry_content)

class EventStoreClient:

    def __init__(self, host, secure=False, port=2113):
        proto = "https" if secure else "http"
        self.base_url = proto + "://" + host + ":" + str(port)

    def post_events(self, stream_name, events):
        url = self.base_url +  "/streams/" + stream_name
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

    def get_stream_page(self, uri):
        headers = {'Accept': 'application/vnd.eventstore.events+json'}
        response = requests.get(uri, headers=headers)
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

    def get_all_events(self, stream_name):
        head = self.get_stream_head(stream_name)
        uri = head.links.get('last', None)
        while uri:
            current_page = self.get_stream_page(uri)
            yield from self.fetch_events(current_page.entries())
            uri = current_page.links.get('previous', None)

    def subscribe(self, stream_name, interval_seconds=1):
        last = self.get_stream_head(stream_name).links['previous']
        while True:
            page = self.get_stream_page(last)
            yield from self.fetch_events(page.entries())

            current = page.links.get('previous', last)
            if last == current:
                time.sleep(interval_seconds)
            last = current
