from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from ddtrace import tracer
from ddtrace.internal.utils import get_argument_value
from ddtrace.llmobs._constants import INPUT_VALUE
from ddtrace.llmobs._constants import NAME
from ddtrace.llmobs._constants import OUTPUT_VALUE
from ddtrace.llmobs._constants import SPAN_KIND
from ddtrace.llmobs._constants import SPAN_LINKS
from ddtrace.llmobs._integrations.base import BaseLLMIntegration
from ddtrace.llmobs._utils import _get_llmobs_parent_id
from ddtrace.llmobs._utils import _get_nearest_llmobs_ancestor
from ddtrace.span import Span


node_invokes: Dict[str, Any] = {}


class LangGraphIntegration(BaseLLMIntegration):
    _integration_name = "langgraph"

    def _llmobs_set_tags(
        self,
        span: Span,
        args: List[Any],
        kwargs: Dict[str, Any],
        response: Optional[Any] = None,
        operation: str = "",  # oneof graph, node
        **kw: Dict[str, Any],
    ):
        if not self.llmobs_enabled:
            return

        inputs = get_argument_value(args, kwargs, 0, "input")
        span_name = kw.get("name", span.name)

        span._set_ctx_items(
            {
                SPAN_KIND: "agent",  # should nodes be workflows? should it be dynamic to if a subgraph is included?
                INPUT_VALUE: inputs,
                OUTPUT_VALUE: response,
                NAME: span_name,
            }
        )

        if operation != "node":
            return  # we set the graph span links in handle_pregel_loop_tick

        config = get_argument_value(args, kwargs, 1, "config")

        metadata = config.get("metadata", {}) if isinstance(config, dict) else {}
        node_instance_id = metadata["langgraph_checkpoint_ns"].split(":")[-1]

        node_invoke = node_invokes[node_instance_id] = node_invokes.get(node_instance_id, {})
        node_invoke["span"] = {
            "trace_id": "{:x}".format(span.trace_id),
            "span_id": str(span.span_id),
        }

        node_invoke_span_links = node_invoke.get("from")

        span_links = (
            [
                {
                    "span_id": str(_get_llmobs_parent_id(span)) or "undefined",
                    "trace_id": "{:x}".format(span.trace_id),
                    # we assume no span link means it is the first node of a graph
                    "attributes": {
                        "from": "input",
                        "to": "input",
                    },
                }
            ]
            if node_invoke_span_links is None
            else node_invoke_span_links
        )

        current_span_links = span._get_ctx_item(SPAN_LINKS) or []
        span._set_ctx_item(SPAN_LINKS, current_span_links + span_links)

    def handle_pregel_loop_tick(self, finished_tasks: dict, next_tasks: dict, more_tasks: bool):
        if not self.llmobs_enabled:
            return

        graph_span = (
            tracer.current_span()
        )  # since we're running the the pregel loop, and not in a node, the graph span should be the current span
        graph_caller = _get_nearest_llmobs_ancestor(graph_span) if graph_span else None

        if not more_tasks and graph_span is not None:
            span_links = [
                {**node_invokes[task_id]["span"], "attributes": {"from": "output", "to": "output"}}
                for task_id in finished_tasks.keys()
            ]

            current_span_links = graph_span._get_ctx_item(SPAN_LINKS) or []
            graph_span._set_ctx_item(SPAN_LINKS, current_span_links + span_links)

            if graph_caller is not None:
                current_graph_caller_span_links = graph_caller._get_ctx_item(SPAN_LINKS) or []
                graph_caller_span_links = [
                    {
                        "span_id": str(graph_span.span_id) or "undefined",
                        "trace_id": "{:x}".format(graph_caller.trace_id),
                        "attributes": {
                            "from": "output",
                            "to": "output",
                        },
                    }
                ]
                graph_caller._set_ctx_item(SPAN_LINKS, current_graph_caller_span_links + graph_caller_span_links)

            return

        if not finished_tasks and graph_caller is not None:  # first tick of a graph, possibly very brittle logic
            # this is for subgraph logic
            if graph_span is not None:
                current_span_links = graph_span._get_ctx_item(SPAN_LINKS) or []
                graph_span._set_ctx_item(
                    SPAN_LINKS,
                    current_span_links
                    + [
                        {
                            "span_id": str(_get_llmobs_parent_id(graph_span)) or "undefined",
                            "trace_id": "{:x}".format(graph_caller.trace_id),
                            "attributes": {
                                "from": "input",
                                "to": "input",
                            },
                        }
                    ],
                )

        parent_node_names_to_ids = {task.name: task_id for task_id, task in finished_tasks.items()}

        for task_id, task in next_tasks.items():
            task_config = getattr(task, "config", {})
            task_triggers = task_config.get("metadata", {}).get("langgraph_triggers", [])

            def extract_parent(trigger):
                split = trigger.split(":")
                if len(split) < 3:
                    return split[0]
                return split[1]

            parent_node_names = [extract_parent(trigger) for trigger in task_triggers]
            parent_ids: List[str] = [
                parent_node_names_to_ids.get(parent_node_name, "") for parent_node_name in parent_node_names
            ]

            for parent_id in parent_ids:
                parent_span = node_invokes.get(parent_id, {}).get("span")
                if not parent_span:
                    continue
                parent_span_link = {
                    **node_invokes.get(parent_id, {}).get("span", {}),
                    "attributes": {
                        "from": "output",
                        "to": "input",
                    },
                }
                node_invoke = node_invokes[task_id] = node_invokes.get(task_id, {})
                from_nodes = node_invoke["from"] = node_invoke.get("from", [])

                from_nodes.append(parent_span_link)
