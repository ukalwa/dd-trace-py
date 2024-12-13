import random
import unittest

import dramatiq
import pytest

from ddtrace.contrib.dramatiq import patch
from ddtrace.contrib.dramatiq import unpatch
from ddtrace.pin import Pin
from tests.utils import DummyTracer
from tests.utils import snapshot


class DramatiqSnapshotTests(unittest.TestCase):
    def setUp(self):
        patch()

    def tearDown(self):
        unpatch()

    @snapshot(wait_for_num_traces=2)
    def test_idempotent_patch(self):
        # calling patch() twice doesn't have side effects
        patch()

        @dramatiq.actor
        def fn_task():
            return "idempotent patch result"

        fn_task.send()
        fn_task.send_with_options(options={"max_retries": 1})

    def test_idempotent_unpatch(self):
        # calling unpatch() multiple times doesn't have side effects
        unpatch()
        unpatch()

        tracer = DummyTracer()
        Pin(tracer=tracer).onto(dramatiq)

        @dramatiq.actor
        def fn_task():
            return "idempotent unpatch result"

        fn_task.send()
        fn_task.send_with_options(options={"max_retries": 1})

        spans = tracer.pop()
        assert len(spans) == 0

    def test_fn_task_synchronous(self):
        # the body of the function is not instrumented so calling it
        # directly doesn't create a trace
        tracer = DummyTracer()
        Pin(tracer=tracer).onto(dramatiq)

        @dramatiq.actor
        def fn_task():
            return "synchronous task"

        fn_task()

        spans = tracer.pop()
        assert len(spans) == 0

    @snapshot(wait_for_num_traces=2)
    def test_fn_task_send(self):
        # it should execute a traced task with a returning value
        @dramatiq.actor
        def fn_task():
            return "asynchronous task"

        fn_task.send()
        fn_task.send_with_options(options={"max_retries": 1})

    @snapshot(wait_for_num_traces=2)
    def test_fn_task_send_with_params(self):
        # it should execute a traced async task that has parameters
        @dramatiq.actor
        def fn_task(a: int, b: int) -> int:
            return a + b

        fn_task.send(1, 2)
        fn_task.send_with_options(args=(1, 2), options={"max_retries": 1})

    @snapshot(wait_for_num_traces=1)
    def test_fn_exception_no_retries(self):
        # it should not catch exceptions in task functions
        @dramatiq.actor
        def fn_task():
            raise ValueError("test error")

        fn_task.send()

    # Ignoring these two values due to variance in method name
    # Python 3.7 - 3.9 -> send_with_options
    # Python 3.10+ -> Actor.send_with_options
    @snapshot(ignores=["meta.error.message", "meta.error.stack"], wait_for_num_traces=1)
    def test_send_exception(self):
        # it should catch exceptions generated by send/send_with_options
        @dramatiq.actor
        def fn_task(a: int, b: int) -> int:
            return a + b

        # send() with invalid params
        with pytest.raises(TypeError):
            fn_task.send_with_options([])


@snapshot(wait_for_num_traces=1)
def test_fn_retry_exception(stub_broker, stub_worker):
    # it should not catch retry exceptions in task functions
    patch()
    failures, successes = [], []

    @dramatiq.actor(max_retries=1)
    def fn_task():
        if len(failures) == 0:
            failures.append(1)
            raise RuntimeError("First failure.")
        else:
            successes.append(1)

    fn_task.send()
    stub_broker.join(queue_name=fn_task.queue_name, fail_fast=True)
    stub_worker.join()

    assert len(successes) == 1
    assert len(failures) == 1
    unpatch()


# def test_my_flaky_test():
#     r = random.randint(1, 10)
#     assert r % 2 == 0
