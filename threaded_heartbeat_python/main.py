from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.worker import Worker
from contextvars import copy_context
from datetime import datetime, timedelta
from typing import Any, Type, TypeVar, Callable, Awaitable, cast
from concurrent.futures import ThreadPoolExecutor
import temporalio.activity
import temporalio.exceptions
import asyncio
import threading
import temporalio.api.common.v1
import temporalio.client
import temporalio.converter
import temporalio.worker
import temporalio.workflow
import time


F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def auto_heartbeat(fn: F) -> F:
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)
    return cast(F, wrapper)


class _WorkflowInboundInterceptor(
    temporalio.worker.WorkflowInboundInterceptor
):
    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        self.next.init(_WorkflowOutboundInterceptor(outbound))

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        return await self.next.execute_workflow(input)

    async def handle_signal(self, input: temporalio.worker.HandleSignalInput) -> None:
        return await self.next.handle_signal(input)

    async def handle_query(self, input: temporalio.worker.HandleQueryInput) -> Any:
        return await self.next.handle_query(input)

    def handle_update_validator(
        self, input: temporalio.worker.HandleUpdateInput
    ) -> None:
        self.next.handle_update_validator(input)

    async def handle_update_handler(
        self, input: temporalio.worker.HandleUpdateInput
    ) -> Any:
        return await self.next.handle_update_handler(input)


class Interceptor(
    temporalio.client.Interceptor, temporalio.worker.Interceptor
):
    """Interceptor that can serialize/deserialize contexts."""

    def __init__(
        self,
        executor: ThreadPoolExecutor,
        payload_converter: temporalio.converter.PayloadConverter = temporalio.converter.default().payload_converter,
    ) -> None:
        self._payload_converter = payload_converter
        self._executor = executor

    def intercept_client(
        self, next: temporalio.client.OutboundInterceptor
    ) -> temporalio.client.OutboundInterceptor:
        return _ClientOutboundInterceptor(
            next, self._payload_converter
        )

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _ActivityInboundInterceptor(next, self._executor)

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> Type[_WorkflowInboundInterceptor]:
        return _WorkflowInboundInterceptor


class _ClientOutboundInterceptor(
    temporalio.client.OutboundInterceptor
):
    def __init__(
        self,
        next: temporalio.client.OutboundInterceptor,
        payload_converter: temporalio.converter.PayloadConverter,
    ) -> None:
        super().__init__(next)
        self._payload_converter = payload_converter

    async def start_workflow(
        self, input: temporalio.client.StartWorkflowInput
    ) -> temporalio.client.WorkflowHandle[Any, Any]:
        return await super().start_workflow(input)

    async def query_workflow(self, input: temporalio.client.QueryWorkflowInput) -> Any:
        return await super().query_workflow(input)

    async def signal_workflow(
        self, input: temporalio.client.SignalWorkflowInput
    ) -> None:
        await super().signal_workflow(input)

    async def start_workflow_update(
        self, input: temporalio.client.StartWorkflowUpdateInput
    ) -> temporalio.client.WorkflowUpdateHandle[Any]:
        return await self.next.start_workflow_update(input)




class _ActivityInboundInterceptor(
    temporalio.worker.ActivityInboundInterceptor
):
    def __init__(
            self,
            next: temporalio.worker.ActivityInboundInterceptor,
            executor: ThreadPoolExecutor,
    ):
        super().__init__(next)
        self._executor = executor

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        guard_heartbeat = "auto_heartbeat" in input.fn.__qualname__
        stop_heartbeat_event = None
        heartbeat_timeout = activity.info().heartbeat_timeout
        if heartbeat_timeout and guard_heartbeat:
            stop_heartbeat_event = threading.Event()
            ctx = copy_context()
            asyncio.get_event_loop().run_in_executor(self._executor, ctx.run, heartbeat_every, stop_heartbeat_event,
                                                     heartbeat_timeout.total_seconds() / 2)
        try:
            return await self.next.execute_activity(input)
        finally:
            if guard_heartbeat:
                print("stopping heartbeats") # For demonstration purposes only. Feel free to remove.
                stop_heartbeat_event.set()




class _WorkflowOutboundInterceptor(
    temporalio.worker.WorkflowOutboundInterceptor
):
    async def signal_child_workflow(
        self, input: temporalio.worker.SignalChildWorkflowInput
    ) -> None:
        return await self.next.signal_child_workflow(input)

    async def signal_external_workflow(
        self, input: temporalio.worker.SignalExternalWorkflowInput
    ) -> None:
        return await self.next.signal_external_workflow(input)

    def start_activity(
        self, input: temporalio.worker.StartActivityInput
    ) -> temporalio.workflow.ActivityHandle:
        return self.next.start_activity(input)

    async def start_child_workflow(
        self, input: temporalio.worker.StartChildWorkflowInput
    ) -> temporalio.workflow.ChildWorkflowHandle:
        return await self.next.start_child_workflow(input)

    def start_local_activity(
        self, input: temporalio.worker.StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle:
        return self.next.start_local_activity(input)


def heartbeat_every(stop_event: threading.Event, delay: float, *details: Any) -> None:
    # Heartbeat every so often while not cancelled
    while True:
        time.sleep(delay)
        activity.heartbeat(*details)
        print(f"Heartbeat at {datetime.now()}")  # This is for demonstration purposes only. Feel free to remove.
        if stop_event.is_set():
            break
    return


@activity.defn
@auto_heartbeat
def run_forever_activity():
    # Wait forever, catch the cancel, and return some value
    while True:
        print("Hello from run_forever_activity...")
        time.sleep(1)


@workflow.defn
class WaitForCancelWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Start activity and wait on it (it will get cancelled from signal)
        self.activity = workflow.start_activity(
            run_forever_activity,
            start_to_close_timeout=timedelta(hours=20),
            # We set a heartbeat timeout so Temporal knows if the activity
            # failed/crashed. If we don't heartbeat within this time, Temporal
            # will consider the activity failed.
            heartbeat_timeout=timedelta(seconds=10),
            # Tell the activity not to retry for demonstration purposes only
            retry_policy=RetryPolicy(maximum_attempts=1),
            # Tell the workflow to wait for the post-cancel result
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        return await self.activity

    @workflow.signal
    def cancel_activity(self) -> None:
        self.activity.cancel()


interrupt_event = asyncio.Event()


async def main():
    # Connect client
    executor = ThreadPoolExecutor(max_workers=5)
    client = await Client.connect(
        "localhost:7233",
        # Use an interceptor to automatically heartbeat functions decoarted w/ autoheartbeat using threads
        interceptors=[Interceptor(executor)],
    )
    # Run a worker for the workflow
    async with Worker(
        client,
        task_queue="custom_decorator-task-queue",
        activities=[run_forever_activity],
        workflows=[WaitForCancelWorkflow],
        # Synchronous activities are not allowed unless we provide some kind of
        # executor. This same thread pool could be passed to multiple workers if
        # desired.
        activity_executor=executor,
    ):
        # Wait until interrupted
        print("Activity Worker started, ctrl+c to exit")

        handle = await client.start_workflow(
            WaitForCancelWorkflow.run,
            id=f"custom_decorator-workflow-id",
            task_queue="custom_decorator-task-queue",
        )

        print("Started workflow, waiting 5 seconds before cancelling")
        await asyncio.sleep(5)
        print("cancelling workflow")
        await handle.cancel()
        result = await handle.result()
        print(f"Result: {result}")

        await interrupt_event.wait()
        print("Shutting down")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        interrupt_event.set()
        loop.run_until_complete(loop.shutdown_asyncgens())