import os
import sys

import langgraph

from ddtrace import config
from ddtrace.contrib.trace_utils import unwrap
from ddtrace.contrib.trace_utils import with_traced_module
from ddtrace.contrib.trace_utils import wrap
from ddtrace.internal.utils import get_argument_value
from ddtrace.llmobs._integrations.langgraph import LangGraphIntegration
from ddtrace.pin import Pin


def get_version():
    return getattr(langgraph, "__version__", "")


config._add(
    "langgraph",
    {
        "span_prompt_completion_sample_rate": float(os.getenv("DD_LANGGRAPH_SPAN_PROMPT_COMPLETION_SAMPLE_RATE", 1.0)),
        "span_char_limit": int(os.getenv("DD_LANGGRAPH_SPAN_CHAR_LIMIT", 128)),
    },
)


@with_traced_module
def traced_runnable_seq_invoke(langgraph, pin, func, instance, args, kwargs):
    """
    Traces a specific invocation of a RunnableSeq, which represents a node in a graph.
    Although this API is usable elsewhere, internal to LangGraph it is used to represent the
    main node invocation (function, graph, callable), the channel write, and then any routing logic.

    We should be able to utilize the `instance.steps` to grab the first step as the node, and any `_route`
    steps as routing logic.

    One caveat is that if the first task is a graph (LangGraph), we should skip tracing at this step, as
    we will trace the graph invocation separately with `traced_pregel_invoke`. For proper span linking logic,
    we will mark the config for that graph as a subgraph invoke.
    """
    integration: LangGraphIntegration = langgraph._datadog_integration

    node_name = instance.steps[0].name

    if node_name in ("_write", "_route"):
        return func(*args, **kwargs)

    if node_name == "LangGraph":
        config = get_argument_value(args, kwargs, 1, "config", optional=True) or {}
        config.get("metadata", {})["subgraph"] = True
        return func(*args, **kwargs)

    span = integration.trace(
        pin,
        "%s.%s.%s" % (instance.__module__, instance.__class__.__name__, node_name),
        submit_to_llmobs=True,
        interface_type="agent",
    )

    result = None

    try:
        result = func(*args, **kwargs)
    except Exception:
        span.set_exc_info(*sys.exc_info())
        integration.metric(span, "incr", "request.error", 1)
        raise
    finally:
        integration.llmobs_set_tags(span, args=args, kwargs=kwargs, response=result, operation="node")
        span.finish()

    return result


@with_traced_module
def traced_pregel_invoke(langgraph, pin, func, instance, args, kwargs):
    """
    Trace the invocation of a Pregel (CompiledGraph) instance.
    This operation represents the parent execution of an individual graph.
    This graph could be standalone, or embedded as a subgraph in a node of a larger graph.
    Under the hood, this graph will `tick` through until all computed tasks are completed.
    """
    integration: LangGraphIntegration = langgraph._datadog_integration
    span = integration.trace(
        pin,
        "%s.%s.%s" % (instance.__module__, instance.__class__.__name__, instance.name),
        submit_to_llmobs=True,
        interface_type="agent",
    )

    result = None

    try:
        result = func(*args, **kwargs)
    except Exception:
        span.set_exc_info(*sys.exc_info())
        integration.metric(span, "incr", "request.error", 1)
        raise
    finally:
        integration.llmobs_set_tags(
            span, args=args, kwargs={**kwargs, "name": instance.name}, response=result, operation="graph"
        )
        span.finish()

    return result


@with_traced_module
def patched_pregel_loop_tick(langgraph, pin, func, instance, args, kwargs):
    """
    Patch the pregel loop tick. No tracing is done, and processing only happens if LLM Observability is enabled.
    The underlying `handle_pregel_loop_tick` function adds span links between specific node invocations in the graph.
    """
    integration: LangGraphIntegration = langgraph._datadog_integration

    finished_tasks = getattr(instance, "tasks", {})
    result = func(*args, **kwargs)
    next_tasks = getattr(instance, "tasks", {})  # they should have been updated at this point

    is_subgraph = instance.config.get("metadata", {}).get("subgraph", False)

    integration.handle_pregel_loop_tick(finished_tasks, next_tasks, result, is_subgraph)

    return result


def patch():
    if getattr(langgraph, "_datadog_patch", False):
        return

    langgraph._datadog_patch = True

    Pin().onto(langgraph)
    integration = LangGraphIntegration(integration_config=config.langgraph)
    langgraph._datadog_integration = integration

    wrap("langgraph", "utils.runnable.RunnableSeq.invoke", traced_runnable_seq_invoke(langgraph))
    wrap("langgraph", "pregel.Pregel.invoke", traced_pregel_invoke(langgraph))
    wrap("langgraph", "pregel.loop.PregelLoop.tick", patched_pregel_loop_tick(langgraph))


def unpatch():
    if not getattr(langgraph, "_datadog_patch", False):
        return

    langgraph._datadog_patch = False

    unwrap(langgraph.utils.runnable.RunnableSeq, "invoke")
    unwrap(langgraph.pregel.Pregel, "invoke")
    unwrap(langgraph.pregel.loop.PregelLoop, "tick")

    delattr(langgraph, "_datadog_integration")
