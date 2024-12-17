from enum import Enum
from typing import List
from typing import Optional
from typing import Tuple

from ddtrace.internal.ci_visibility.telemetry.constants import CIVISIBILITY_TELEMETRY_NAMESPACE as _NAMESPACE
from ddtrace.internal.ci_visibility.telemetry.constants import EVENT_TYPES
from ddtrace.internal.ci_visibility.telemetry.constants import TEST_FRAMEWORKS
from ddtrace.internal.logger import get_logger
from ddtrace.internal.telemetry import telemetry_writer


log = get_logger(__name__)


class EVENTS_TELEMETRY(str, Enum):
    CREATED = "event_created"
    FINISHED = "event_finished"
    MANUAL_API_EVENT = "manual_api_events"
    ENQUEUED_FOR_SERIALIZATION = "events_enqueued_for_serialization"


def _record_event(
    event: EVENTS_TELEMETRY,
    event_type: EVENT_TYPES,
    test_framework: Optional[TEST_FRAMEWORKS],
    has_codeowners: Optional[bool] = False,
    is_unsupported_ci: Optional[bool] = False,
    early_flake_detection_abort_reason: Optional[str] = None,
):
    log.debug(
        "Recording event telemetry: event=%s"
        ", event_type=%s"
        ", test_framework=%s"
        ", has_codeowners=%s"
        ", is_unsuported_ci=%s"
        ", early_flake_detection_abort_reason=%s",
        event,
        event_type,
        test_framework,
        has_codeowners,
        is_unsupported_ci,
        early_flake_detection_abort_reason,
    )
    if event_type == EVENT_TYPES.TEST:
        log.warning("Test events should be recorded with record_event_test_created or record_event_test_finished")
        return

    if has_codeowners and event_type != EVENT_TYPES.SESSION:
        log.debug("has_codeowners tag can only be set for sessions, but event type is %s", event_type)
    if is_unsupported_ci and event_type != EVENT_TYPES.SESSION:
        log.debug("unsupported_ci tag can only be set for sessions, but event type is %s", event_type)

    if early_flake_detection_abort_reason and (
        event_type not in [EVENT_TYPES.SESSION] or event != EVENTS_TELEMETRY.FINISHED
    ):
        log.debug(
            "early_flake_detection_abort_reason tag can only be set for tests and session finish events",
        )

    _tags: List[Tuple[str, str]] = [("event_type", event_type.value)]
    if test_framework and test_framework != TEST_FRAMEWORKS.MANUAL:
        _tags.append(("test_framework", str(test_framework.value)))
    if event_type == EVENT_TYPES.SESSION:
        _tags.append(("has_codeowners", "1" if has_codeowners else "0"))
        _tags.append(("is_unsupported_ci", "1" if has_codeowners else "0"))

    if early_flake_detection_abort_reason and event == EVENTS_TELEMETRY.FINISHED and event_type == EVENT_TYPES.SESSION:
        _tags.append(("early_flake_detection_abort_reason", early_flake_detection_abort_reason))

    telemetry_writer.add_count_metric(_NAMESPACE, event.value, 1, tuple(_tags))


def record_event_created(
    event_type: EVENT_TYPES,
    test_framework: TEST_FRAMEWORKS,
    has_codeowners: Optional[bool] = None,
    is_unsupported_ci: Optional[bool] = None,
):
    if event_type == EVENT_TYPES.TEST:
        log.warning("Test events should be recorded with record_event_test_created")
        return

    if test_framework == TEST_FRAMEWORKS.MANUAL:
        # manual API usage is tracked only by way of tracking created events
        record_manual_api_event_created(event_type)

    _record_event(
        event=EVENTS_TELEMETRY.CREATED,
        event_type=event_type,
        test_framework=test_framework,
        has_codeowners=has_codeowners,
        is_unsupported_ci=is_unsupported_ci,
    )


def record_event_finished(
    event_type: EVENT_TYPES,
    test_framework: Optional[TEST_FRAMEWORKS],
    has_codeowners: bool = False,
    is_unsupported_ci: bool = False,
    early_flake_detection_abort_reason: Optional[str] = None,
):
    if event_type == EVENT_TYPES.TEST:
        log.warning("Test events should be recorded with record_event_test_finished")
        return

    _record_event(
        event=EVENTS_TELEMETRY.FINISHED,
        event_type=event_type,
        test_framework=test_framework,
        has_codeowners=has_codeowners,
        is_unsupported_ci=is_unsupported_ci,
        early_flake_detection_abort_reason=early_flake_detection_abort_reason,
    )


def record_manual_api_event_created(event_type: EVENT_TYPES):
    # Note: _created suffix is added in cases we were to change the metric name in the future.
    # The current metric applies to event creation even though it does not specify it
    telemetry_writer.add_count_metric(_NAMESPACE, EVENTS_TELEMETRY.MANUAL_API_EVENT, 1, (("event_type", event_type),))


def record_events_enqueued_for_serialization(events_count: int):
    telemetry_writer.add_count_metric(_NAMESPACE, EVENTS_TELEMETRY.ENQUEUED_FOR_SERIALIZATION, events_count)


def record_event_created_test(
    test_framework: Optional[TEST_FRAMEWORKS],
    is_benchmark: bool = False,
):
    log.debug("Recording test event created: test_framework=%s, is_benchmark=%s", test_framework, is_benchmark)
    tags: List[Tuple[str, str]] = [("event_type", EVENT_TYPES.TEST)]

    if test_framework and test_framework != TEST_FRAMEWORKS.MANUAL:
        tags.append(("test_framework", str(test_framework.value)))
    elif test_framework == TEST_FRAMEWORKS.MANUAL:
        record_manual_api_event_created(EVENT_TYPES.TEST)

    if is_benchmark:
        tags.append(("is_benchmark", "true"))

    telemetry_writer.add_count_metric(_NAMESPACE, EVENTS_TELEMETRY.FINISHED, 1, tuple(tags))


def record_event_finished_test(
    test_framework: Optional[TEST_FRAMEWORKS],
    is_new: bool = False,
    is_retry: bool = False,
    early_flake_detection_abort_reason: Optional[str] = None,
    is_rum: bool = False,
    browser_driver: Optional[str] = None,
    is_benchmark: bool = False,
):
    log.debug(
        "Recording test event finished: test_framework=%s"
        ", is_new=%s"
        ", is_retry=%s"
        ", early_flake_detection_abort_reason=%s"
        ", is_rum=%s"
        ", browser_driver=%s"
        ", is_benchmark=%s",
        test_framework,
        is_new,
        is_retry,
        early_flake_detection_abort_reason,
        is_rum,
        browser_driver,
        is_benchmark,
    )

    tags: List[Tuple[str, str]] = [("event_type", EVENT_TYPES.TEST)]

    if test_framework is not None:
        tags.append(("test_framework", test_framework))
    if is_benchmark:
        tags.append(("is_benchmark", "true"))
    if is_new:
        tags.append(("is_new", "true"))
    if is_retry:
        tags.append(("is_retry", "true"))
    if is_rum:
        tags.append(("is_rum", "true"))
    if browser_driver is not None:
        tags.append(("browser_driver", browser_driver))
    if early_flake_detection_abort_reason is not None:
        tags.append(("early_flake_detection_abort_reason", early_flake_detection_abort_reason))

    telemetry_writer.add_count_metric(_NAMESPACE, EVENTS_TELEMETRY.FINISHED, 1, tuple(tags))
