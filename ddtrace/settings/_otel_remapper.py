import os

from ..constants import ENV_KEY
from ..constants import VERSION_KEY
from ..internal.logger import get_logger


log = get_logger(__name__)


OTEL_UNIFIED_TAG_MAPPINGS = {
    "deployment.environment": ENV_KEY,
    "service.name": "service",
    "service.version": VERSION_KEY,
}


def _remap_otel_log_level(otel_value):
    """Remaps the otel log level to ddtrace log level"""
    if otel_value == "debug":
        return "True"
    elif otel_value == "info":
        return "False"
    else:
        log.warning(
            "ddtrace does not support otel log level '%s'. setting ddtrace to log level info.",
            otel_value,
        )
        return "False"


def _remap_otel_propagators(otel_value):
    """Remaps the otel propagators to ddtrace propagators"""
    accepted_styles = []
    for style in otel_value.split(","):
        style = style.strip().lower()
        if style in ["b3", "b3multi", "b3single", "datadog", "tracecontext", "none"]:
            if style == "b3single":
                style = "b3"
            if style not in accepted_styles:
                accepted_styles.append(style)
        else:
            log.warning("Following style not supported by ddtrace: %s.", style)
    return ",".join(accepted_styles)


def _remap_traces_sampler(otel_value):
    """Remaps the otel trace sampler to ddtrace trace sampler"""
    if otel_value in ["always_on", "always_off", "traceidratio"]:
        log.warning("Trace sampler set to %s; only parent based sampling is supported.", otel_value)
    if otel_value == "always_on" or otel_value == "parentbased_always_on":
        return "1.0"
    elif otel_value == "always_off" or otel_value == "parentbased_always_off":
        return "0.0"
    elif otel_value == "traceidratio" or otel_value == "parentbased_traceidratio":
        return os.environ.get("OTEL_TRACES_SAMPLER_ARG", "1")
    else:
        return otel_value


def _remap_traces_exporter(otel_value):
    """Remaps the otel trace exporter to ddtrace trace enabled"""
    if otel_value == "none":
        return "False"
    log.warning(
        "A trace exporter value '%s' is set, but not supported. Traces will be exported to Datadog.", otel_value
    )
    return ""


def _remap_metrics_exporter(otel_value):
    """Remaps the otel metrics exporter to ddtrace metrics exporter"""
    if otel_value != "none":
        log.warning(
            "An unrecognized runtime metrics exporter '%s' is being "
            "used; setting DD_RUNTIME_METRICS_ENABLED to False.",
            otel_value,
        )
    return "False"


def _remap_logs_exporter(otel_value):
    """Logs warning when OTEL Logs exporter is configured. DDTRACE does not support this configuration."""
    if otel_value != "none":
        log.warning(
            "Unsupported OTEL logs exporter value detected: %s. Only the 'none' value is supported.", otel_value
        )
        return ""
    return ""


def _remap_otel_tags(otel_value):
    """Remaps the otel tags to ddtrace tags"""
    dd_tags = []
    remaining_tags = []
    otel_tags = otel_value.split(",")
    otel_user_tag_dict = dict()

    try:
        for tag in otel_tags:
            tag_pair = tag.split("=")
            otel_user_tag_dict[tag_pair[0]] = tag_pair[1]

        for otel_key, dd_key in OTEL_UNIFIED_TAG_MAPPINGS.items():
            if otel_key in otel_user_tag_dict.keys():
                dd_tags.append("{}:{}".format(dd_key, otel_user_tag_dict[otel_key]))

        for key, value in otel_user_tag_dict.items():
            if key.lower() not in OTEL_UNIFIED_TAG_MAPPINGS.keys():
                if len(dd_tags) < 10:
                    dd_tags.append("{}:{}".format(key, value))
                else:
                    remaining_tags.append("{}:{}".format(key, value))
    except Exception:
        log.warning("DDTRACE failed to read OTEL_RESOURCE_ATTRIBUTES. This value is misformatted: %s", otel_value)

    if len(otel_user_tag_dict.items()) > 10:
        log.warning(
            "To preserve metrics cardinality, only the following first 10 tags have been processed %s. "
            "The following tags were not ingested: %s",
            dd_tags,
            remaining_tags,
        )
    return ",".join(dd_tags)


def _remap_otel_sdk_config(otel_value):
    """Remaps the otel sdk config to ddtrace sdk config"""
    if otel_value == "false":
        return "True"
    elif otel_value == "true":
        return "False"
    else:
        log.warning("OTEL_SDK_DISABLED='%s'  is not supported", otel_value)
        return otel_value


def _remap_default(otel_value):
    """Remaps the otel default value to ddtrace default value"""
    return otel_value


ENV_VAR_MAPPINGS = {
    "OTEL_SERVICE_NAME": ("DD_SERVICE", _remap_default),
    "OTEL_LOG_LEVEL": ("DD_TRACE_DEBUG", _remap_otel_log_level),
    "OTEL_PROPAGATORS": ("DD_TRACE_PROPAGATION_STYLE", _remap_otel_propagators),
    "OTEL_TRACES_SAMPLER": ("DD_TRACE_SAMPLE_RATE", _remap_traces_sampler),
    "OTEL_TRACES_EXPORTER": ("DD_TRACE_ENABLED", _remap_traces_exporter),
    "OTEL_METRICS_EXPORTER": ("DD_RUNTIME_METRICS_ENABLED", _remap_metrics_exporter),
    "OTEL_LOGS_EXPORTER": ("", _remap_logs_exporter),  # Does not set a DDTRACE environment variable.
    "OTEL_RESOURCE_ATTRIBUTES": ("DD_TAGS", _remap_otel_tags),
    "OTEL_SDK_DISABLED": ("DD_TRACE_OTEL_ENABLED", _remap_otel_sdk_config),
}


def otel_remapping():
    """Checks for the existence of both OTEL and Datadog tracer environment variables and remaps them accordingly.
    Datadog Environment variables take precedence over OTEL, but if there isn't a Datadog value present,
    then OTEL values take their place.
    """
    user_envs = {key.upper(): value for key, value in os.environ.items()}

    for otel_env, otel_value in user_envs.items():
        if otel_env not in ENV_VAR_MAPPINGS.keys():
            continue

        dd_env, otel_config_remapper = ENV_VAR_MAPPINGS[otel_env]
        if dd_env in user_envs:
            log.debug(
                "Datadog configuration %s is already set. OpenTelemetry configuration will be ignored: %s=%s",
                dd_env,
                otel_env,
                otel_value,
            )
            continue

        if otel_env not in ["OTEL_RESOURCE_ATTRIBUTES", "OTEL_SERVICE_NAME"]:
            # Resource attributes and service name are case-insensitive
            otel_value = otel_value.lower()

        mapped_value = otel_config_remapper(otel_value)
        if mapped_value != "":
            os.environ[dd_env] = mapped_value
            log.debug(
                "OpenTelemetry configuration %s has been remapped to ddtrace configuration %s=%s",
                otel_env,
                dd_env,
                mapped_value,
            )