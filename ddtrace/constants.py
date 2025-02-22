from ddtrace.internal.utils.deprecations import DDTraceDeprecationWarning as _DDTraceDeprecationWarning
from ddtrace.vendor import debtcollector as _debtcollector


# TODO: Deprecate and remove the SAMPLE_RATE_METRIC_KEY constant.
# This key enables legacy trace sampling support in the Datadog agent.
SAMPLE_RATE_METRIC_KEY = "_sample_rate"
SAMPLING_PRIORITY_KEY = "_sampling_priority_v1"
_ANALYTICS_SAMPLE_RATE_KEY = ANALYTICS_SAMPLE_RATE_KEY = "_dd1.sr.eausr"
SAMPLING_AGENT_DECISION = "_dd.agent_psr"
SAMPLING_RULE_DECISION = "_dd.rule_psr"
SAMPLING_LIMIT_DECISION = "_dd.limit_psr"
_SINGLE_SPAN_SAMPLING_MECHANISM = "_dd.span_sampling.mechanism"
_SINGLE_SPAN_SAMPLING_RATE = "_dd.span_sampling.rule_rate"
_SINGLE_SPAN_SAMPLING_MAX_PER_SEC = "_dd.span_sampling.max_per_second"
_SINGLE_SPAN_SAMPLING_MAX_PER_SEC_NO_LIMIT = -1
_APM_ENABLED_METRIC_KEY = "_dd.apm.enabled"

ORIGIN_KEY = "_dd.origin"
USER_ID_KEY = "_dd.p.usr.id"
HOSTNAME_KEY = "_dd.hostname"
RUNTIME_FAMILY = "_dd.runtime_family"
ENV_KEY = "env"
VERSION_KEY = "version"
SERVICE_KEY = "service.name"
BASE_SERVICE_KEY = "_dd.base_service"
SERVICE_VERSION_KEY = "service.version"
SPAN_KIND = "span.kind"
SPAN_MEASURED_KEY = "_dd.measured"
KEEP_SPANS_RATE_KEY = "_dd.tracer_kr"
MULTIPLE_IP_HEADERS = "_dd.multiple-ip-headers"

APPSEC_ENV = "DD_APPSEC_ENABLED"
CONFIG_ENDPOINT_ENV = "_DD_CONFIG_ENDPOINT"
CONFIG_ENDPOINT_RETRIES_ENV = "_DD_CONFIG_ENDPOINT_RETRIES"
CONFIG_ENDPOINT_TIMEOUT_ENV = "_DD_CONFIG_ENDPOINT_TIMEOUT"
IAST_ENV = "DD_IAST_ENABLED"

MANUAL_DROP_KEY = "manual.drop"
MANUAL_KEEP_KEY = "manual.keep"

ERROR_MSG = "error.message"  # a string representing the error message
ERROR_TYPE = "error.type"  # a string representing the type of the error
ERROR_STACK = "error.stack"  # a human readable version of the stack.

PID = "process_id"

# Use this to explicitly inform the backend that a trace should be rejected and not stored.
USER_REJECT = -1
# Used by the builtin sampler to inform the backend that a trace should be rejected and not stored.
AUTO_REJECT = 0
# Used by the builtin sampler to inform the backend that a trace should be kept and stored.
AUTO_KEEP = 1
# Use this to explicitly inform the backend that a trace should be kept and stored.
USER_KEEP = 2


_DEPRECATED_MODULE_ATTRIBUTES = [
    "ANALYTICS_SAMPLE_RATE_KEY",
]


def __getattr__(name):
    if name in _DEPRECATED_MODULE_ATTRIBUTES:
        _debtcollector.deprecate(
            ("%s.%s is deprecated" % (__name__, name)),
            category=_DDTraceDeprecationWarning,
            removal_version="3.0.0",
        )

    if name in globals():
        return globals()[name]

    raise AttributeError("%s has no attribute %s", __name__, name)
