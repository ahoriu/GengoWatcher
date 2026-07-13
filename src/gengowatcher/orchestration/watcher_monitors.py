"""Optional-monitor thread helpers extracted from GengoWatcher.

Owns the three side-channel monitor threads that the orchestrator's
``run()`` method spawns alongside the RSS and WebSocket monitors:

* run_email_monitor(watcher)         -- EmailMonitor-backed worker.
* run_website_monitor(watcher)       -- WebsiteMonitor-backed worker.
* run_native_browser_listener(watcher)
  -- NativeBrowserListener + StateProjector pipeline.

The watcher keeps thin delegator methods on the class so the
existing ``threading.Thread(target=self._run_*)`` call sites in
``GengoWatcher.run()`` continue to resolve them through the
instance.
"""

from __future__ import annotations

import asyncio
import threading
import time

try:
    from ..email_monitor import EmailMonitor
except ImportError:  # pragma: no cover - email monitor optional
    EmailMonitor = None

try:
    from ..native_browser_listener import NativeBrowserListener
except ImportError:  # pragma: no cover - native listener optional
    NativeBrowserListener = None

try:
    from ..state_projector import StateProjector
except ImportError:  # pragma: no cover - state projector optional
    StateProjector = None

try:
    from ..website_monitor import WebsiteMonitor
except ImportError:  # pragma: no cover - website monitor optional
    WebsiteMonitor = None


def _run_async_monitor(watcher, monitor_class, attribute_name, error_context):
    """Run an optional async monitor in a dedicated thread and event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    checker_stop = threading.Event()
    shutdown_thread = None

    async def job_callback(job_id, title, reward, url, source):
        await asyncio.to_thread(
            watcher._process_new_job, job_id, title, reward, url, source
        )

    try:
        monitor = monitor_class(
            config=watcher.config,
            logger=watcher.logger,
            job_callback=job_callback,
            shutdown_event=asyncio.Event(),
        )
        setattr(watcher, attribute_name, monitor)

        def check_shutdown():
            while not checker_stop.wait(1):
                if watcher.shutdown_event.is_set():
                    loop.call_soon_threadsafe(monitor.shutdown_event.set)
                    return

        shutdown_thread = threading.Thread(target=check_shutdown, daemon=True)
        shutdown_thread.start()
        loop.run_until_complete(monitor.start())
    except Exception:
        watcher.logger.exception("%s monitor failed", error_context)
    finally:
        checker_stop.set()
        if shutdown_thread is not None:
            shutdown_thread.join()
        asyncio.set_event_loop(None)
        loop.close()


def run_email_monitor(watcher):
    """Run email monitor in a dedicated thread with its own event loop."""
    if EmailMonitor is None:
        watcher.logger.error("Email monitor dependencies not installed")
        return
    _run_async_monitor(watcher, EmailMonitor, "email_monitor", "Email")


def run_website_monitor(watcher):
    """Run website monitor in a dedicated thread with its own event loop."""
    if WebsiteMonitor is None:
        watcher.logger.error("Website monitor dependencies not installed (playwright)")
        return
    _run_async_monitor(watcher, WebsiteMonitor, "website_monitor", "Website")


def run_native_browser_listener(watcher):
    """Run native browser listener loop - drains events into state projector."""
    from queue import Empty

    watcher.logger.info("Native browser listener starting...")
    try:
        while not watcher.shutdown_event.is_set():
            try:
                # Poll native listener (publishes events)
                if hasattr(watcher, "_native_listener"):
                    watcher._native_listener.run_once()

                # Drain events into state projector
                if hasattr(watcher, "_state_projector"):
                    try:
                        from ..event_bus import get_native_events_queue
                        from ..events import EventEnvelope

                        q = get_native_events_queue()
                        while True:
                            try:
                                event_dict = q.get_nowait()
                                event = EventEnvelope.from_dict(event_dict)
                                watcher._state_projector.project(event)
                            except Empty:
                                break
                            except Exception as e:
                                watcher.logger.debug(f"Event projection error: {e}")
                    except Exception as e:
                        watcher.logger.debug(f"Event bus drain error: {e}")

            except Exception as e:
                watcher.logger.debug(f"Native browser listener error: {e}")
            capture_interval = (
                getattr(watcher, "_native_listener", None).capture_interval
                if hasattr(watcher, "_native_listener")
                and hasattr(
                    getattr(watcher, "_native_listener", None), "capture_interval"
                )
                else 0.75
            )
            time.sleep(capture_interval)
    finally:
        listener = getattr(watcher, "_native_listener", None)
        close = getattr(listener, "close", None)
        if not callable(close):
            close = getattr(listener, "stop", None)
        if callable(close):
            try:
                close()
            except Exception:
                watcher.logger.exception("Native browser listener shutdown failed")


__all__ = [
    "run_email_monitor",
    "run_native_browser_listener",
    "run_website_monitor",
]
