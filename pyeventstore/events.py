import asyncio

import aiohttp

from pyeventstore.stream_page import StreamPage

class Subscription:
    def __init__(self, event_queue):
        self.event_queue = event_queue

    @asyncio.coroutine
    def get(self):
        return (yield from self.event_queue.get())

@asyncio.coroutine
def get_stream_page(uri):
    # print('getting stream page {}'.format(uri))
    headers = {'Accept': 'application/vnd.eventstore.events+json'}
    response = yield from aiohttp.request('get', uri, headers=headers)
    content = yield from response.json()
    # print('received stream page {}'.format(uri))
    return StreamPage(content)


@asyncio.coroutine
def fetch_event(uri):
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
        task = asyncio.Task(fetch_event(entry.links['alternate']))
        coroutines.append(task)

    return (yield from asyncio.gather(*coroutines))


@asyncio.coroutine
def get_all_events(head_uri):
    page_queue = asyncio.Queue(5)
    event_queue = asyncio.Queue(20)

    head = yield from get_stream_page(head_uri)
    if 'last' in head.links:
        last = yield from get_stream_page(head.links['last'])
    else:
        last = head

    @asyncio.coroutine
    def follow_previous_links():
        current_page = last
        while 'previous' in current_page.links:
            yield from page_queue.put(current_page)
            previous_uri = current_page.links['previous']
            current_page = yield from get_stream_page(previous_uri)
        yield from page_queue.put(None)  # indicate last page

    @asyncio.coroutine
    def fetch_events():
        while True:
            page = yield from page_queue.get()
            if page is None:  # last page
                yield from event_queue.put(None)
                return
            events = yield from get_all_events_from_page(page)
            for event in events:
                yield from event_queue.put(event)

    asyncio.async(follow_previous_links())
    asyncio.async(fetch_events())

    return Subscription(event_queue)
