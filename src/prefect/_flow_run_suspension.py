from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
from contextlib import asynccontextmanager, contextmanager
from functools import partial
from typing import TYPE_CHECKING, AsyncGenerator, Callable, Generator
from uuid import UUID

from prefect.client.schemas.objects import State
from prefect.exceptions import Pause
from prefect.logging.loggers import get_logger

if TYPE_CHECKING:
    from prefect._internal.observers import FlowRunSuspendingObserver
    from prefect.context import SettingsContext


class FlowRunSuspensionRequest:
    """
    Mutable in-process suspension request shared by flow, child flow, and task contexts.

    The stored state is the server-accepted `Suspended` state that should be raised
    through Prefect's existing `Pause` control-flow path at the next orchestration
    boundary.
    """

    def __init__(self) -> None:
        self._suspended_state: State | None = None
        self._lock = threading.Lock()

    def mark_requested(self, state: State) -> None:
        with self._lock:
            self._suspended_state = state

    def get_state(self) -> State | None:
        with self._lock:
            return self._suspended_state

    def is_requested(self) -> bool:
        return self.get_state() is not None

    def raise_if_requested(self) -> None:
        if state := self.get_state():
            raise Pause(state=state)


def is_suspended_flow_run_state(state: State | None) -> bool:
    return bool(state and state.is_paused() and state.name == "Suspended")


_active_flow_run_suspension_requests: dict[UUID, FlowRunSuspensionRequest] = {}
_active_flow_run_suspension_requests_lock = threading.Lock()


@contextmanager
def register_flow_run_suspension_request(
    flow_run_id: UUID,
    suspension_request: FlowRunSuspensionRequest,
) -> Generator[None, None, None]:
    with _active_flow_run_suspension_requests_lock:
        _active_flow_run_suspension_requests[flow_run_id] = suspension_request

    try:
        yield
    finally:
        with _active_flow_run_suspension_requests_lock:
            if (
                _active_flow_run_suspension_requests.get(flow_run_id)
                is suspension_request
            ):
                _active_flow_run_suspension_requests.pop(flow_run_id, None)


def mark_flow_run_suspension_requested(flow_run_id: UUID, state: State) -> bool:
    with _active_flow_run_suspension_requests_lock:
        suspension_request = _active_flow_run_suspension_requests.get(flow_run_id)

    if suspension_request is None:
        return False

    suspension_request.mark_requested(state)
    return True


def raise_if_flow_run_suspension_requested() -> None:
    from prefect.context import FlowRunContext

    if flow_run_context := FlowRunContext.get():
        flow_run_context.flow_run_suspension_request.raise_if_requested()


class _SharedFlowRunSuspensionObserver:
    """
    Runs one `FlowRunSuspendingObserver` on a dedicated thread for every flow run
    of the process that observes suspension under the same settings.

    The observer gets its own thread and event loop so that it keeps receiving
    suspension events while flow code holds a caller's thread, for example when
    an async flow calls sync tasks. Sharing it means a process that runs many
    flow runs at once holds one events subscription and one thread, instead of
    one of each per flow run.
    """

    def __init__(self, settings_context: SettingsContext, polling_interval: float):
        self.key = (id(settings_context.settings), polling_interval)
        self.users = 0
        self._settings_context = settings_context
        self._polling_interval = polling_interval
        self._lock = threading.Lock()
        self._stop_requested = False
        self._wake_observer: Callable[[], object] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._observer: FlowRunSuspendingObserver | None = None
        self._suspension_requests: dict[UUID, FlowRunSuspensionRequest] = {}
        self._started: concurrent.futures.Future[None] = concurrent.futures.Future()
        self.exited: concurrent.futures.Future[None] = concurrent.futures.Future()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        """Ask the observer to shut down; `exited` completes once it has."""
        with self._lock:
            self._stop_requested = True
            if self._wake_observer is not None:
                self._wake_observer()

    def watch(
        self, flow_run_id: UUID, suspension_request: FlowRunSuspensionRequest
    ) -> concurrent.futures.Future[None]:
        """
        Start watching a flow run.

        The returned future completes once the initial state check of the flow
        run has completed or the observer has failed.
        """
        with self._lock:
            self._suspension_requests[flow_run_id] = suspension_request
        watched: concurrent.futures.Future[None] = concurrent.futures.Future()

        def watch_once_started(_: concurrent.futures.Future[None]) -> None:
            with self._lock:
                loop = self._loop
            if loop is None:
                _set_result(watched)
                return
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._watch(flow_run_id), loop
                )
            except RuntimeError:
                # The observer's loop has closed.
                _set_result(watched)
                return
            future.add_done_callback(lambda _: _set_result(watched))

        self._started.add_done_callback(watch_once_started)
        return watched

    def unwatch(self, flow_run_id: UUID) -> None:
        with self._lock:
            self._suspension_requests.pop(flow_run_id, None)
            loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(self._remove_watch, flow_run_id)
        except RuntimeError:
            # The observer's loop has closed, and its watches with it.
            pass

    async def _watch(self, flow_run_id: UUID) -> None:
        if self._observer is not None:
            await self._observer.watch_flow_run_id(flow_run_id)

    def _remove_watch(self, flow_run_id: UUID) -> None:
        if self._observer is not None:
            self._observer.remove_in_flight_flow_run_id(flow_run_id)

    def _mark_suspended(self, flow_run_id: UUID, state: State) -> None:
        if mark_flow_run_suspension_requested(flow_run_id, state):
            return
        with self._lock:
            suspension_request = self._suspension_requests.get(flow_run_id)
        if suspension_request is not None:
            suspension_request.mark_requested(state)

    async def _observe(self) -> None:
        from prefect._internal.observers import FlowRunSuspendingObserver

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        with self._lock:
            if self._stop_requested:
                stop_event.set()
            self._wake_observer = partial(loop.call_soon_threadsafe, stop_event.set)

        try:
            async with FlowRunSuspendingObserver(
                on_suspended=self._mark_suspended,
                polling_interval=self._polling_interval,
            ) as observer:
                self._observer = observer
                with self._lock:
                    self._loop = loop
                _set_result(self._started)
                await stop_event.wait()
        finally:
            # Under the lock so that `stop` and `unwatch` never schedule onto a
            # closed loop.
            with self._lock:
                self._wake_observer = None
                self._loop = None

    def _run_with_settings(self) -> None:
        with self._settings_context.model_copy():
            asyncio.run(self._observe())

    def _run(self) -> None:
        try:
            # A fresh context, so the observer does not keep the context of the
            # flow run that happened to start it.
            contextvars.Context().run(self._run_with_settings)
        except Exception:
            get_logger("flow_run_suspension").debug(
                "Flow run suspension observer exited with an exception",
                exc_info=True,
            )
        finally:
            _set_result(self._started)
            _set_result(self.exited)


_shared_observers: dict[tuple[int, float], _SharedFlowRunSuspensionObserver] = {}
_shared_observers_lock = threading.Lock()


def _acquire_shared_observer(
    polling_interval: float,
) -> _SharedFlowRunSuspensionObserver:
    from prefect.context import get_settings_context

    settings_context = get_settings_context()
    key = (id(settings_context.settings), polling_interval)
    with _shared_observers_lock:
        observer = _shared_observers.get(key)
        if observer is None or observer.exited.done():
            observer = _SharedFlowRunSuspensionObserver(
                settings_context, polling_interval
            )
            _shared_observers[key] = observer
            observer.start()
        observer.users += 1
    return observer


def _release_shared_observer(observer: _SharedFlowRunSuspensionObserver) -> bool:
    """Release one use of the observer, and stop it if it was the last one."""
    with _shared_observers_lock:
        observer.users -= 1
        if observer.users:
            return False
        if _shared_observers.get(observer.key) is observer:
            del _shared_observers[observer.key]
    observer.stop()
    return True


@contextmanager
def observe_flow_run_suspension(
    flow_run_id: UUID,
    suspension_request: FlowRunSuspensionRequest,
    polling_interval: float = 10,
) -> Generator[None, None, None]:
    observer = _acquire_shared_observer(polling_interval)
    try:
        observer.watch(flow_run_id, suspension_request).result()
        yield
    finally:
        observer.unwatch(flow_run_id)
        if _release_shared_observer(observer):
            try:
                observer.exited.result(timeout=2)
            except concurrent.futures.TimeoutError:
                pass


@asynccontextmanager
async def observe_flow_run_suspension_async(
    flow_run_id: UUID,
    suspension_request: FlowRunSuspensionRequest,
    polling_interval: float = 10,
) -> AsyncGenerator[None, None]:
    """
    Async counterpart of `observe_flow_run_suspension`.

    Waits for the observer to start and to shut down without blocking the event
    loop, so other work on the loop, such as other flow runs, keeps running
    meanwhile.
    """
    observer = _acquire_shared_observer(polling_interval)
    try:
        await _wait_without_blocking(observer.watch(flow_run_id, suspension_request))
        yield
    finally:
        observer.unwatch(flow_run_id)
        if _release_shared_observer(observer):
            await asyncio.wait([_wait_without_blocking(observer.exited)], timeout=2)


def _wait_without_blocking(
    future: concurrent.futures.Future[None],
) -> asyncio.Future[None]:
    loop = asyncio.get_running_loop()
    waiter: asyncio.Future[None] = loop.create_future()

    def resolve_threadsafe(_: concurrent.futures.Future[None]) -> None:
        try:
            loop.call_soon_threadsafe(_resolve_future, waiter)
        except RuntimeError:
            # The loop has closed, so nothing is waiting on the future anymore.
            pass

    future.add_done_callback(resolve_threadsafe)
    return waiter


def _set_result(future: concurrent.futures.Future[None]) -> None:
    try:
        future.set_result(None)
    except concurrent.futures.InvalidStateError:
        pass


def _resolve_future(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)
