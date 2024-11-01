import os
import sys

import google.generativeai as genai

from ddtrace import config
from ddtrace.contrib.internal.google_generativeai._utils import TracedAsyncGenerateContentResponse
from ddtrace.contrib.internal.google_generativeai._utils import TracedGenerateContentResponse
from ddtrace.contrib.internal.google_generativeai._utils import _extract_api_key
from ddtrace.contrib.internal.google_generativeai._utils import _extract_model_name
from ddtrace.contrib.internal.google_generativeai._utils import get_system_instruction_parts
from ddtrace.contrib.internal.google_generativeai._utils import get_generation_config_dict
from ddtrace.contrib.internal.google_generativeai._utils import tag_request
from ddtrace.contrib.internal.google_generativeai._utils import tag_response
from ddtrace.contrib.internal.google_generativeai._utils import GOOGLE_GENERATIVEAI
from ddtrace.contrib.trace_utils import unwrap
from ddtrace.contrib.trace_utils import with_traced_module
from ddtrace.contrib.trace_utils import wrap
from ddtrace.llmobs._integrations import GeminiIntegration
from ddtrace.pin import Pin


config._add(
    "genai",
    {
        "span_prompt_completion_sample_rate": float(
            os.getenv("DD_GOOGLE_GENERATIVEAI_SPAN_PROMPT_COMPLETION_SAMPLE_RATE", 1.0)
        ),
        "span_char_limit": int(os.getenv("DD_GOOGLE_GENERATIVEAI_SPAN_CHAR_LIMIT", 128)),
    },
)


def get_version():
    # type: () -> str
    return getattr(genai, "__version__", "")


@with_traced_module
def traced_generate(genai, pin, func, instance, args, kwargs):
    integration = genai._datadog_integration
    stream = kwargs.get("stream", False)
    generations = None
    span = integration.trace(
        pin,
        "%s.%s" % (instance.__class__.__name__, func.__name__),
        provider="google",
        model=_extract_model_name(instance),
        submit_to_llmobs=True,
    )
    try:
        tag_request(span, integration, args, kwargs, GOOGLE_GENERATIVEAI, get_system_instruction_parts(instance), get_generation_config_dict(instance, kwargs))
        generations = func(*args, **kwargs)
        api_key = _extract_api_key(instance)
        if api_key:
            span.set_tag("%s.request.api_key" % GOOGLE_GENERATIVEAI, "...{}".format(api_key[-4:]))
        if stream:
            return TracedGenerateContentResponse(generations, instance, integration, span, args, kwargs)
        tag_response(GOOGLE_GENERATIVEAI, span, generations, integration, instance)
    except Exception:
        span.set_exc_info(*sys.exc_info())
        raise
    finally:
        # streamed spans will be finished separately once the stream generator is exhausted
        if span.error or not stream:
            kwargs["instance"] = instance
            integration.llmobs_set_tags(span, args=args, kwargs=kwargs, response=generations)
            span.finish()
    return generations


@with_traced_module
async def traced_agenerate(genai, pin, func, instance, args, kwargs):
    integration = genai._datadog_integration
    stream = kwargs.get("stream", False)
    generations = None
    span = integration.trace(
        pin,
        "%s.%s" % (instance.__class__.__name__, func.__name__),
        provider="google",
        model=_extract_model_name(instance),
        submit_to_llmobs=True,
    )
    try:
        tag_request(span, integration, instance, args, kwargs)
        generations = await func(*args, **kwargs)
        if stream:
            return TracedAsyncGenerateContentResponse(generations, instance, integration, span, args, kwargs)
        tag_response(span, generations, integration, instance)
    except Exception:
        span.set_exc_info(*sys.exc_info())
        raise
    finally:
        # streamed spans will be finished separately once the stream generator is exhausted
        if span.error or not stream:
            kwargs["instance"] = instance
            integration.llmobs_set_tags(span, args=args, kwargs=kwargs, response=generations)
            span.finish()
    return generations


def patch():
    if getattr(genai, "_datadog_patch", False):
        return

    genai._datadog_patch = True

    Pin().onto(genai)
    integration = GeminiIntegration(integration_config=config.genai)
    genai._datadog_integration = integration

    wrap("google.generativeai", "GenerativeModel.generate_content", traced_generate(genai))
    wrap("google.generativeai", "GenerativeModel.generate_content_async", traced_agenerate(genai))


def unpatch():
    if not getattr(genai, "_datadog_patch", False):
        return

    genai._datadog_patch = False

    unwrap(genai.GenerativeModel, "generate_content")
    unwrap(genai.GenerativeModel, "generate_content_async")

    delattr(genai, "_datadog_integration")
