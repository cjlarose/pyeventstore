import json
import uuid
import time
import asyncio

import requests
from requests.exceptions import HTTPError
import aiohttp

def links_as_dict(links):
    return {link['relation']: link['uri'] for link in links}

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

@asyncio.coroutine
def get_stream_page_async(uri):
    # print('getting stream page {}'.format(uri))
    headers = {'Accept': 'application/vnd.eventstore.events+json'}
    response = yield from aiohttp.request('get', uri, headers=headers)
    content = yield from response.json()
    # print('received stream page {}'.format(uri))
    return StreamPage(content)

@asyncio.coroutine
def fetch_event_async(uri):
    # print('getting event data from {}'.format(uri))
    headers = {'Accept': 'application/json'}
    response = yield from aiohttp.request('get', uri, headers=headers)
    content = yield from response.json()
    # print('received content from {}'.format(uri))
    return (uri, content)

@asyncio.coroutine
def get_all_events_from_page(page):
    coroutines = []
    for entry in page.entries():
        task = asyncio.Task(fetch_event_async(entry.links['alternate']))
        coroutines.append(task)

    return (yield from asyncio.gather(*coroutines))

@asyncio.coroutine
def get_all_events_async(head_uri, on_event):
    q = asyncio.Queue()
    head = yield from get_stream_page_async(head_uri)
    if 'last' in head.links:
        last = yield from get_stream_page_async(head.links['last'])
    else:
        last = head

    @asyncio.coroutine
    def follow_previous_links():
        current_page = last
        while 'previous' in current_page.links:
            yield from q.put(current_page)
            previous_uri = current_page.links['previous']
            current_page = yield from get_stream_page_async(previous_uri)
        yield from q.put(None) # indicate last page

    @asyncio.coroutine
    def fetch_events():
        while True:
            page = yield from q.get()
            if page is None: # last page
                return
            events = yield from get_all_events_from_page(page)
            for event in events:
                on_event(event)

    asyncio.async(follow_previous_links())
    yield from fetch_events()

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
