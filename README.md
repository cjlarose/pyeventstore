# Python Event Store client

Experimental client library for [Event Store][1]. The package depends on
`asyncio`, and so can be used with Python 3.3 and above only.

[1]: https://geteventstore.com/

## Examples

Reading all events from a stream

```python
import pyeventstore, asyncio
event_store = pyeventstore.Client("192.168.59.103")

def handle_events():
    subscription = yield from event_store.get_all_events('ponies')
    while True:
        event = yield from subscription.get()
        if event is None:  # reached end of stream
            break
        print(event)

loop = asyncio.get_event_loop()
loop.run_until_complete(handle_events())
```

Subscribing to only new events from a stream looks pretty similar:

```python
import pyeventstore, asyncio
event_store = pyeventstore.Client("192.168.59.103")

def handle_events():
    subscription = yield from event_store.subscribe('ponies')
    while True:
        event = yield from subscription.get()
        print(event)

loop = asyncio.get_event_loop()
loop.run_until_complete(handle_events())
```
