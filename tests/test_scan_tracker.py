import asyncio
import json
from types import SimpleNamespace

import pytest

from hdf5_reader_service.scan_tracker import ScanTracker


@pytest.fixture
def mock_stomp(monkeypatch):
    """Patch stomp.Connection so we can instantiate ScanTracker without a broker."""

    class MockConnection:
        def __init__(self, *args, **kwargs):
            self.listener = None
            self.subscriptions = []
            self.connected = False

        def set_listener(self, name, listener):
            self.listener = listener

        def connect(self, username, password, wait=True):
            self.connected = True

        def subscribe(self, destination, id, ack):
            self.subscriptions.append(destination)

        def disconnect(self):
            self.connected = False

    monkeypatch.setattr(
        "hdf5_reader_service.scan_tracker.stomp.Connection", MockConnection
    )
    return MockConnection


@pytest.mark.asyncio
async def test_start_and_stop_update_state_and_broadcast(mock_stomp):
    loop = asyncio.get_running_loop()
    tracker = ScanTracker(
        host="dummy",
        port=1234,
        username="guest",
        password="guest",
        destination="public.worker.event",
        loop=loop,
    )

    # Ensure STOMP was "connected"
    assert tracker._conn.connected  # type: ignore
    assert "/topic/public.worker.event" in tracker._conn.subscriptions  # type: ignore

    # Create a dummy subscriber
    subscriber_queue = asyncio.Queue()
    tracker._subscribers.add(subscriber_queue)

    # Simulate receiving a "start" message
    start_doc = {
        "name": "start",
        "doc": {
            "uid": "abc123",
            "data_session_directory": "/data/test",
            "scan_file": "scan001",
        },
    }
    frame = SimpleNamespace(body=json.dumps(start_doc))
    tracker.on_message(frame)

    # Wait for broadcast
    msg = await asyncio.wait_for(subscriber_queue.get(), timeout=1)
    assert msg["uuid"] == "abc123"
    assert msg["status"] == "running"
    assert msg["filepath"] == "/data/test/scan001.nxs"

    # Verify internal state
    latest = tracker.get_latest()
    assert latest.uuid == "abc123"
    assert latest.status == "running"

    # Simulate receiving a "stop" message
    stop_doc = {
        "name": "stop",
        "doc": {"run_start": "abc123", "exit_status": "success"},
    }
    frame = SimpleNamespace(body=json.dumps(stop_doc))
    tracker.on_message(frame)

    msg2 = await asyncio.wait_for(subscriber_queue.get(), timeout=1)
    assert msg2["status"] == "finished"

    latest = tracker.get_latest()
    assert latest.status == "finished"

    tracker.disconnect()
    assert not tracker._conn.connected  # type: ignore


@pytest.mark.asyncio
async def test_listen_generator_yields_messages(mock_stomp):
    loop = asyncio.get_running_loop()
    tracker = ScanTracker("dummy", 1234, "guest", "guest", "topic", loop)

    # Create generator
    agen = tracker.listen()

    # Start generator so the queue is added
    # Use __anext__ but don't block indefinitely
    future = asyncio.create_task(agen.__anext__())

    # Small delay to ensure generator has registered its queue
    await asyncio.sleep(0.01)

    # Broadcast message: now generator's queue exists
    await tracker._broadcast({"uuid": "xyz", "status": "running"})

    # Await the result from generator
    msg = await asyncio.wait_for(future, timeout=1)
    assert msg["uuid"] == "xyz"

    await agen.aclose()


@pytest.mark.asyncio
async def test_ignores_irrelevant_stop(mock_stomp):
    loop = asyncio.get_running_loop()
    tracker = ScanTracker("dummy", 1234, "guest", "guest", "topic", loop)

    tracker._latest.uuid = "somethingelse"
    stop_doc = {
        "name": "stop",
        "doc": {"run_start": "abc123", "exit_status": "success"},
    }
    frame = SimpleNamespace(body=json.dumps(stop_doc))
    # should not raise, should not broadcast
    tracker.on_message(frame)
    assert tracker.get_latest().uuid == "somethingelse"
