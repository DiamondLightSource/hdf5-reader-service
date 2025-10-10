import asyncio
import json
import logging
import threading
from dataclasses import asdict, dataclass

import stomp

logger = logging.getLogger("scantracker")


@dataclass
class LatestScan:
    uuid: str | None = None
    filepath: str | None = None
    status: str = "idle"  # idle | running | finished | failed


class ScanTracker(stomp.ConnectionListener):
    """
    Encapsulates a STOMP connection and maintains the latest scan state.
    - `loop` must be the asyncio event loop running the FastAPI app (pass from lifespan)
    - `destination` may be either a full STOMP destination (starts with "/") or a bare
      topic name
      in which case we subscribe to "/topic/{destination}".
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        destination: str,
        loop: asyncio.AbstractEventLoop,
    ):
        self.loop = loop
        self._latest_lock = threading.Lock()
        self._latest = LatestScan()
        # subscribers are asyncio.Queue objects accessed only from event-loop coroutines
        self._subscribers: set[asyncio.Queue] = set()

        # create and connect STOMP connection (stomp handles internal threads)
        self._conn = stomp.Connection([(host, port)])
        self._conn.set_listener("", self)
        try:
            self._conn.connect(username, password, wait=True)
        except Exception:
            logger.exception("Failed to connect to STOMP broker")
            raise

        # normalize destination
        dest = destination if destination.startswith("/") else f"/topic/{destination}"
        self._conn.subscribe(destination=dest, id="1", ack="auto")
        logger.info(f"ScanTracker subscribed to {dest}")

    # ----------------------
    # Public API
    # ----------------------
    def get_latest(self) -> LatestScan:
        with self._latest_lock:
            # return a copy so callers don't mutate internal state
            return LatestScan(**self._latest.__dict__)

    def disconnect(self):
        try:
            self._conn.disconnect()
        except Exception:
            logger.exception("Error when disconnecting STOMP connection")

    # ----------------------
    # STOMP callback (runs in stomp thread)
    # ----------------------
    def on_message(self, frame):
        """
        Called by stomp.py thread. Convert the message, update internal state,
        and schedule an async broadcast on the FastAPI event loop.
        """
        try:
            body = json.loads(frame.body)
        except Exception:
            logger.exception("Failed to parse JSON body")
            return

        name = body.get("name")
        doc = body.get("doc", {})

        # Only handle start/stop for now
        if name == "start":
            uid = doc.get("uid")
            data_dir = doc.get("data_session_directory")
            scan_file = doc.get("scan_file")
            if uid and data_dir and scan_file:
                filepath = f"{data_dir}/{scan_file}.nxs"
                with self._latest_lock:
                    self._latest.uuid = uid
                    self._latest.filepath = filepath
                    self._latest.status = "running"
                snapshot = asdict(self.get_latest())
                # schedule broadcast to event-loop subscribers
                asyncio.run_coroutine_threadsafe(self._broadcast(snapshot), self.loop)
                logger.info(f"Scan started: {uid} -> {filepath}")
            else:
                logger.warning("Received start doc with missing fields")

        elif name == "stop":
            run_start = doc.get("run_start")
            exit_status = doc.get("exit_status", "unknown")
            with self._latest_lock:
                if self._latest.uuid != run_start:
                    logger.debug(f"Ignoring stop for unrelated run {run_start}")
                    return
                self._latest.status = (
                    "finished" if exit_status == "success" else "failed"
                )
            snapshot = asdict(self.get_latest())
            asyncio.run_coroutine_threadsafe(self._broadcast(snapshot), self.loop)
            logger.info(f"Scan {run_start} stopped (exit_status={exit_status})")

        else:
            # ignore other messages for now (could handle progress later)
            logger.debug(f"Ignoring message type: {name}")

    # ----------------------
    # Async broadcasting (runs in event loop)
    # ----------------------
    async def _broadcast(self, msg: dict):
        """
        Put `msg` into every subscriber queue. This runs in the event loop (safe).
        """
        if not self._subscribers:
            return
        # copy to avoid mutation during iteration
        subs = list(self._subscribers)
        for q in subs:
            try:
                # do not await many puts serially for speed; but fine for small fanout
                await q.put(msg)
            except Exception:
                logger.exception("Failed to put message into subscriber queue")

    async def listen(self):
        """
        Async generator: yields messages for a single SSE client.
        Usage:
            async for msg in tracker.listen():
                yield f"data: {json.dumps(msg)}\n\n"
        """
        q: asyncio.Queue = asyncio.Queue()
        # register subscriber (only event-loop coroutines touch _subscribers)
        self._subscribers.add(q)
        try:
            while True:
                item = await q.get()
                yield item
        finally:
            # ensure cleanup if client disconnects / generator cancels
            self._subscribers.discard(q)
