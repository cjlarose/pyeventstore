import asyncio

import aiohttp

from pyeventstore.stream_page import StreamPage


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
        yield from q.put(None)  # indicate last page

    @asyncio.coroutine
    def fetch_events():
        while True:
            page = yield from q.get()
            if page is None:  # last page
                return
            events = yield from get_all_events_from_page(page)
            for event in events:
                on_event(event)

    asyncio.async(follow_previous_links())
    yield from fetch_events()
