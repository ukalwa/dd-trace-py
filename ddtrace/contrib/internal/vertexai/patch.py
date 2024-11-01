import os
import sys

import vertexai
from vertexai.generative_models import GenerativeModel
import google.generativeai

from ddtrace import config
from ddtrace.contrib.internal.google_generativeai._utils import _extract_model_name
from ddtrace.contrib.internal.google_generativeai._utils import tag_request
from ddtrace.contrib.internal.google_generativeai._utils import tag_response
from ddtrace.contrib.internal.vertexai._utils import get_system_instruction_parts
from ddtrace.contrib.internal.vertexai._utils import get_generation_config_dict
from ddtrace.contrib.trace_utils import unwrap
from ddtrace.contrib.trace_utils import with_traced_module
from ddtrace.contrib.trace_utils import wrap
from ddtrace.llmobs._integrations import VertexAIIntegration
from ddtrace.pin import Pin

config._add(
    "vertexai",
    {
        "span_prompt_completion_sample_rate": float(
            os.getenv("DD_VERTEXAI_SPAN_PROMPT_COMPLETION_SAMPLE_RATE", 1.0)
        ),
        "span_char_limit": int(os.getenv("DD_VERTEXAI_SPAN_CHAR_LIMIT", 128)),
    },
)

def get_version():
    # type: () -> str
    return getattr(vertexai, "__version__", "")

@with_traced_module
def traced_generate(vertexai, pin, func, instance, args, kwargs):
    integration = vertexai._datadog_integration
    generations = None
    span = integration.trace(
        pin,
        "%s.%s" % (instance.__class__.__name__, func.__name__),
        provider="google",
        model=_extract_model_name(instance),
        submit_to_llmobs=True,
    )
    try:
        tag_request(span, integration, args, kwargs, "vertexai", get_system_instruction_parts(instance), get_generation_config_dict(instance, kwargs))
        generations = func(*args, **kwargs)
        tag_response("vertexai", span, generations, integration, instance)
    except Exception:
        span.set_exc_info(*sys.exc_info())
        raise
    span.finish()
    return generations


def patch():
    if getattr(vertexai, "_datadog_patch", False):
        return

    vertexai._datadog_patch = True

    Pin().onto(vertexai)
    integration = VertexAIIntegration(integration_config=config.vertexai)
    vertexai._datadog_integration = integration

    wrap("vertexai", "generative_models.GenerativeModel.generate_content", traced_generate(vertexai))


def unpatch():
    if not getattr(vertexai, "_datadog_patch", False):
        return

    vertexai._datadog_patch = False

    unwrap(GenerativeModel, "generate_content")

    delattr(vertexai, "_datadog_integration")
