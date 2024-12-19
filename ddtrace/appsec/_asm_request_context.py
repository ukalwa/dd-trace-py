import functools
import json
import re
import sys
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union
from urllib import parse

from ddtrace._trace.span import Span
from ddtrace.appsec._constants import APPSEC
from ddtrace.appsec._constants import EXPLOIT_PREVENTION
from ddtrace.appsec._constants import SPAN_DATA_NAMES
from ddtrace.appsec._utils import add_context_log
from ddtrace.appsec._utils import get_triggers
from ddtrace.internal import core
from ddtrace.internal._exceptions import BlockingException
from ddtrace.internal.constants import REQUEST_PATH_PARAMS
from ddtrace.internal.logger import get_logger
from ddtrace.settings.asm import config as asm_config


if TYPE_CHECKING:
    from ddtrace.appsec._ddwaf import DDWaf_info
    from ddtrace.appsec._ddwaf import DDWaf_result

log = get_logger(__name__)

# Stopgap module for providing ASM context for the blocking features wrapping some contextvars.

if sys.version_info >= (3, 8):
    from typing import Literal  # noqa:F401
else:
    from typing_extensions import Literal  # noqa:F401

_ASM_CONTEXT: Literal["_asm_env"] = "_asm_env"
_WAF_ADDRESSES: Literal["waf_addresses"] = "waf_addresses"
_CALLBACKS: Literal["callbacks"] = "callbacks"
_TELEMETRY: Literal["telemetry"] = "telemetry"
_CONTEXT_CALL: Literal["context"] = "context"
_WAF_CALL: Literal["waf_run"] = "waf_run"
_BLOCK_CALL: Literal["block"] = "block"
_TELEMETRY_WAF_RESULTS: Literal["t_waf_results"] = "t_waf_results"


GLOBAL_CALLBACKS: Dict[str, List[Callable]] = {_CONTEXT_CALL: []}


class ASM_Environment:
    """
    an object of this class contains all asm data (waf and telemetry)
    for a single request. It is bound to a single asm request context.
    It is contained into a ContextVar.
    """

    def __init__(self, span: Optional[Span] = None):
        from ddtrace import tracer

        self.root = not in_asm_context()
        if self.root:
            core.add_suppress_exception(BlockingException)
        # add several layers of fallbacks to get a span, but normal span should be the first or the second one
        context_span = span or core.get_span() or tracer.current_span()
        if context_span is None:
            info = add_context_log(log, "appsec.asm_context.warning::ASM_Environment::no_span")
            log.warning(info)
            context_span = tracer.trace("asm.context")
        self.span: Span = context_span
        self.waf_info: Optional[Callable[[], "DDWaf_info"]] = None
        self.waf_addresses: Dict[str, Any] = {}
        self.callbacks: Dict[str, Any] = {_CONTEXT_CALL: []}
        self.telemetry: Dict[str, Any] = {
            _TELEMETRY_WAF_RESULTS: {
                "blocked": False,
                "triggered": False,
                "timeout": False,
                "version": None,
                "duration": 0.0,
                "total_duration": 0.0,
                "rasp": {
                    "sum_eval": 0,
                    "duration": 0.0,
                    "total_duration": 0.0,
                    "eval": {t: 0 for _, t in EXPLOIT_PREVENTION.TYPE},
                    "match": {t: 0 for _, t in EXPLOIT_PREVENTION.TYPE},
                    "timeout": {t: 0 for _, t in EXPLOIT_PREVENTION.TYPE},
                },
            }
        }
        self.addresses_sent: Set[str] = set()
        self.waf_triggers: List[Dict[str, Any]] = []
        self.blocked: Optional[Dict[str, Any]] = None


def _get_asm_context() -> Optional[ASM_Environment]:
    return core.get_item(_ASM_CONTEXT)


def in_asm_context() -> bool:
    return core.get_item(_ASM_CONTEXT) is not None


def is_blocked() -> bool:
    env = _get_asm_context()
    if env is None:
        return False
    return env.blocked is not None


def get_blocked() -> Dict[str, Any]:
    env = _get_asm_context()
    if env is None:
        return {}
    return env.blocked or {}


def _use_html(headers) -> bool:
    """decide if the response should be html or json.

    Add support for quality values in the Accept header.
    """
    ctype = headers.get("Accept", headers.get("accept", ""))
    if not ctype:
        return False
    html_score = 0.0
    json_score = 0.0
    ctypes = ctype.split(",")
    for ct in ctypes:
        if len(ct) > 128:
            # ignore long (and probably malicious) headers to avoid performances issues
            continue
        m = re.match(r"([^/;]+/[^/;]+)(?:;q=([01](?:\.\d*)?))?", ct.strip())
        if m:
            if m.group(1) == "text/html":
                html_score = max(html_score, min(1.0, float(1.0 if m.group(2) is None else m.group(2))))
            elif m.group(1) == "text/*":
                html_score = max(html_score, min(1.0, float(0.2 if m.group(2) is None else m.group(2))))
            elif m.group(1) == "application/json":
                json_score = max(json_score, min(1.0, float(1.0 if m.group(2) is None else m.group(2))))
            elif m.group(1) == "application/*":
                json_score = max(json_score, min(1.0, float(0.2 if m.group(2) is None else m.group(2))))
    return html_score > json_score


def _ctype_from_headers(block_config, headers) -> None:
    """compute MIME type of the blocked response and store it in the block config"""
    desired_type = block_config.get("type", "auto")
    if desired_type == "auto":
        block_config["content-type"] = "text/html" if _use_html(headers) else "application/json"
    else:
        block_config["content-type"] = "text/html" if block_config["type"] == "html" else "application/json"


def set_blocked(blocked: Dict[str, Any]) -> None:
    blocked = blocked.copy()
    env = _get_asm_context()
    if env is None:
        info = add_context_log(log, "appsec.asm_context.warning::set_blocked::no_active_context")
        log.warning(info)
        return
    _ctype_from_headers(blocked, get_headers())
    env.blocked = blocked


def update_span_metrics(span: Span, name: str, value: Union[float, int]) -> None:
    span.set_metric(name, value + (span.get_metric(name) or 0.0))


def flush_waf_triggers(env: ASM_Environment) -> None:
    # Make sure we find a root span to attach the triggers to
    if env.span is None:
        from ddtrace import tracer

        current_span = tracer.current_span()
        if current_span is None:
            return
        root_span = current_span._local_root or current_span
    else:
        root_span = env.span._local_root or env.span
    if env.waf_triggers:
        report_list = get_triggers(root_span)
        if report_list is not None:
            report_list.extend(env.waf_triggers)
        else:
            report_list = env.waf_triggers
        if asm_config._use_metastruct_for_triggers:
            root_span.set_struct_tag(APPSEC.STRUCT, {"triggers": report_list})
        else:
            root_span.set_tag(APPSEC.JSON, json.dumps({"triggers": report_list}, separators=(",", ":")))
        env.waf_triggers = []
    telemetry_results = get_value(_TELEMETRY, _TELEMETRY_WAF_RESULTS)
    if telemetry_results:
        from ddtrace.appsec._metrics import DDWAF_VERSION

        root_span.set_tag_str(APPSEC.WAF_VERSION, DDWAF_VERSION)
        if telemetry_results["total_duration"]:
            update_span_metrics(root_span, APPSEC.WAF_DURATION, telemetry_results["duration"])
            telemetry_results["duration"] = 0.0
            update_span_metrics(root_span, APPSEC.WAF_DURATION_EXT, telemetry_results["total_duration"])
            telemetry_results["total_duration"] = 0.0
        if telemetry_results["rasp"]["sum_eval"]:
            update_span_metrics(root_span, APPSEC.RASP_DURATION, telemetry_results["rasp"]["duration"])
            telemetry_results["rasp"]["duration"] = 0.0
            update_span_metrics(root_span, APPSEC.RASP_DURATION_EXT, telemetry_results["rasp"]["total_duration"])
            telemetry_results["rasp"]["total_duration"] = 0.0
            update_span_metrics(root_span, APPSEC.RASP_RULE_EVAL, telemetry_results["rasp"]["sum_eval"])
            telemetry_results["rasp"]["sum_eval"] = 0


def finalize_asm_env(env: ASM_Environment) -> None:
    callbacks = GLOBAL_CALLBACKS[_CONTEXT_CALL] + env.callbacks[_CONTEXT_CALL]
    for function in callbacks:
        function(env)
    flush_waf_triggers(env)
    if env.waf_info:
        info = env.waf_info()
        try:
            if info.errors:
                env.span.set_tag_str(APPSEC.EVENT_RULE_ERRORS, info.errors)
                log.debug("appsec.asm_context.debug::finalize_asm_env::waf_errors::%s", info.errors)
            env.span.set_tag_str(APPSEC.EVENT_RULE_VERSION, info.version)
            env.span.set_metric(APPSEC.EVENT_RULE_LOADED, info.loaded)
            env.span.set_metric(APPSEC.EVENT_RULE_ERROR_COUNT, info.failed)
        except Exception:
            log.debug("appsec.asm_context.debug::finalize_asm_env::exception::%s", exc_info=True)

    core.discard_local_item(_ASM_CONTEXT)


def set_value(category: str, address: str, value: Any) -> None:
    env = _get_asm_context()
    if env is None:
        info = add_context_log(log, f"appsec.asm_context.debug::set_value::no_active_context::{category}::{address}")
        log.debug(info)
        return
    asm_context_attr = getattr(env, category, None)
    if asm_context_attr is not None:
        asm_context_attr[address] = value


def set_headers_response(headers: Any) -> None:
    if headers is not None:
        set_waf_address(SPAN_DATA_NAMES.RESPONSE_HEADERS_NO_COOKIES, headers)


def set_body_response(body_response):
    # local import to avoid circular import
    from ddtrace.appsec._utils import parse_response_body

    set_waf_address(SPAN_DATA_NAMES.RESPONSE_BODY, lambda: parse_response_body(body_response))


def set_waf_address(address: str, value: Any) -> None:
    if address == SPAN_DATA_NAMES.REQUEST_URI_RAW:
        parse_address = parse.urlparse(value)
        no_scheme = parse.ParseResult("", "", *parse_address[2:])
        waf_value = parse.urlunparse(no_scheme)
        set_value(_WAF_ADDRESSES, address, waf_value)
    else:
        set_value(_WAF_ADDRESSES, address, value)
    env = _get_asm_context()
    if env and env.span:
        root = env.span._local_root or env.span
        root._set_ctx_item(address, value)


def get_value(category: str, address: str, default: Any = None) -> Any:
    env = _get_asm_context()
    if env is None:
        info = add_context_log(log, f"appsec.asm_context.debug::get_value::no_active_context::{category}::{address}")
        log.debug(info)
        return default
    asm_context_attr = getattr(env, category, None)
    if asm_context_attr is not None:
        return asm_context_attr.get(address, default)
    return default


def get_waf_address(address: str, default: Any = None) -> Any:
    return get_value(_WAF_ADDRESSES, address, default=default)


def add_context_callback(function, global_callback: bool = False) -> None:
    if global_callback:
        callbacks = GLOBAL_CALLBACKS.setdefault(_CONTEXT_CALL, [])
    else:
        callbacks = get_value(_CALLBACKS, _CONTEXT_CALL)
    if callbacks is not None:
        callbacks.append(function)


def remove_context_callback(function, global_callback: bool = False) -> None:
    if global_callback:
        callbacks = GLOBAL_CALLBACKS.get(_CONTEXT_CALL)
    else:
        callbacks = get_value(_CALLBACKS, _CONTEXT_CALL)
    if callbacks:
        callbacks[:] = list([cb for cb in callbacks if cb != function])


def set_waf_callback(value) -> None:
    set_value(_CALLBACKS, _WAF_CALL, value)


def set_waf_info(info: Callable[[], "DDWaf_info"]) -> None:
    env = _get_asm_context()
    if env is None:
        info_str = add_context_log(log, "appsec.asm_context.warning::set_waf_info::no_active_context")
        log.warning(info_str)
        return
    env.waf_info = info


def call_waf_callback(custom_data: Optional[Dict[str, Any]] = None, **kwargs) -> Optional["DDWaf_result"]:
    if not asm_config._asm_enabled:
        return None
    callback = get_value(_CALLBACKS, _WAF_CALL)
    if callback:
        return callback(custom_data, **kwargs)
    else:
        info = add_context_log(log, "appsec.asm_context.warning::call_waf_callback::not_set")
        log.warning(info)
        return None


def set_ip(ip: Optional[str]) -> None:
    if ip is not None:
        set_waf_address(SPAN_DATA_NAMES.REQUEST_HTTP_IP, ip)


def get_ip() -> Optional[str]:
    return get_value(_WAF_ADDRESSES, SPAN_DATA_NAMES.REQUEST_HTTP_IP)


# Note: get/set headers use Any since we just carry the headers here without changing or using them
# and different frameworks use different types that we don't want to force it into a Mapping at the
# early point set_headers is usually called


def set_headers(headers: Any) -> None:
    if headers is not None:
        set_waf_address(SPAN_DATA_NAMES.REQUEST_HEADERS_NO_COOKIES, headers)


def get_headers() -> Optional[Any]:
    return get_value(_WAF_ADDRESSES, SPAN_DATA_NAMES.REQUEST_HEADERS_NO_COOKIES, {})


def set_headers_case_sensitive(case_sensitive: bool) -> None:
    set_waf_address(SPAN_DATA_NAMES.REQUEST_HEADERS_NO_COOKIES_CASE, case_sensitive)


def get_headers_case_sensitive() -> bool:
    return get_value(_WAF_ADDRESSES, SPAN_DATA_NAMES.REQUEST_HEADERS_NO_COOKIES_CASE, False)  # type : ignore


def set_block_request_callable(_callable: Optional[Callable], *_) -> None:
    """
    Sets a callable that could be use to do a best-effort to block the request. If
    the callable need any params, like headers, they should be curried with
    functools.partial.
    """
    if asm_config._asm_enabled and _callable:
        set_value(_CALLBACKS, _BLOCK_CALL, _callable)


def block_request() -> None:
    """
    Calls or returns the stored block request callable, if set.
    """
    _callable = get_value(_CALLBACKS, _BLOCK_CALL)
    if _callable:
        _callable()
    else:
        info = add_context_log(log, "appsec.asm_context.debug::block_request::no_callable")
        log.debug(info)


def get_data_sent() -> Set[str]:
    env = _get_asm_context()
    if env is None:
        info = add_context_log(log, "appsec.asm_context.debug::get_data_sent::no_asm_context")
        log.debug(info)
        return set()
    return env.addresses_sent


def asm_request_context_set(
    remote_ip: Optional[str] = None,
    headers: Any = None,
    headers_case_sensitive: bool = False,
    block_request_callable: Optional[Callable] = None,
) -> None:
    set_ip(remote_ip)
    set_headers(headers)
    set_headers_case_sensitive(headers_case_sensitive)
    set_block_request_callable(block_request_callable)


def set_waf_telemetry_results(
    rules_version: Optional[str],
    is_triggered: bool,
    is_blocked: bool,
    is_timeout: bool,
    rule_type: Optional[str],
    duration: float,
    total_duration: float,
) -> None:
    result = get_value(_TELEMETRY, _TELEMETRY_WAF_RESULTS)
    if result is not None:
        if rule_type is None:
            # Request Blocking telemetry
            result["triggered"] |= is_triggered
            result["blocked"] |= is_blocked
            result["timeout"] |= is_timeout
            if rules_version is not None:
                result["version"] = rules_version
            result["duration"] += duration
            result["total_duration"] += total_duration
        else:
            # Exploit Prevention telemetry
            result["rasp"]["sum_eval"] += 1
            result["rasp"]["eval"][rule_type] += 1
            result["rasp"]["match"][rule_type] += int(is_triggered)
            result["rasp"]["timeout"][rule_type] += int(is_timeout)
            result["rasp"]["duration"] += duration
            result["rasp"]["total_duration"] += total_duration


def get_waf_telemetry_results() -> Optional[Dict[str, Any]]:
    return get_value(_TELEMETRY, _TELEMETRY_WAF_RESULTS)


def store_waf_results_data(data) -> None:
    if not data:
        return
    env = _get_asm_context()
    if env is None:
        info = add_context_log(log, "appsec.asm_context.warning::store_waf_results_data::no_asm_context")
        log.warning(info)
        return
    for d in data:
        d["span_id"] = env.span.span_id
    env.waf_triggers.extend(data)


def start_context(span: Span):
    if asm_config._asm_enabled:
        # it should only be called at start of a core context, when ASM_Env is not set yet
        core.set_item(_ASM_CONTEXT, ASM_Environment(span=span))
        asm_request_context_set(
            core.get_local_item("remote_addr"),
            core.get_local_item("headers"),
            core.get_local_item("headers_case_sensitive"),
            core.get_local_item("block_request_callable"),
        )
    elif asm_config._iast_enabled:
        core.set_item(_ASM_CONTEXT, ASM_Environment())


def end_context(span: Span):
    env = _get_asm_context()
    if env is not None and env.span is span:
        finalize_asm_env(env)


def _on_context_ended(ctx):
    env = ctx.get_local_item(_ASM_CONTEXT)
    if env is not None:
        finalize_asm_env(env)


def _on_wrapped_view(kwargs):
    return_value = [None, None]
    # if Appsec is enabled, we can try to block as we have the path parameters at that point
    if asm_config._asm_enabled and in_asm_context():
        log.debug("Flask WAF call for Suspicious Request Blocking on request")
        if kwargs:
            set_waf_address(REQUEST_PATH_PARAMS, kwargs)
        call_waf_callback()
        if is_blocked():
            callback_block = get_value(_CALLBACKS, "flask_block")
            return_value[0] = callback_block

    # If IAST is enabled, taint the Flask function kwargs (path parameters)
    from ddtrace.appsec._iast._utils import _is_iast_enabled

    if _is_iast_enabled() and kwargs:
        from ddtrace.appsec._iast._iast_request_context import is_iast_request_enabled
        from ddtrace.appsec._iast._taint_tracking import OriginType
        from ddtrace.appsec._iast._taint_tracking._taint_objects import taint_pyobject

        if not is_iast_request_enabled():
            return return_value

        _kwargs = {}
        for k, v in kwargs.items():
            _kwargs[k] = taint_pyobject(
                pyobject=v, source_name=k, source_value=v, source_origin=OriginType.PATH_PARAMETER
            )
        return_value[1] = _kwargs
    return return_value


def _on_set_request_tags(request, span, flask_config):
    from ddtrace.appsec._iast._utils import _is_iast_enabled

    if _is_iast_enabled():
        from ddtrace.appsec._iast._iast_request_context import is_iast_request_enabled
        from ddtrace.appsec._iast._metrics import _set_metric_iast_instrumented_source
        from ddtrace.appsec._iast._taint_tracking import OriginType
        from ddtrace.appsec._iast._taint_utils import taint_structure

        _set_metric_iast_instrumented_source(OriginType.COOKIE_NAME)
        _set_metric_iast_instrumented_source(OriginType.COOKIE)

        if not is_iast_request_enabled():
            return

        request.cookies = taint_structure(
            request.cookies,
            OriginType.COOKIE_NAME,
            OriginType.COOKIE,
            override_pyobject_tainted=True,
        )


def _on_pre_tracedrequest(ctx):
    current_span = ctx.span
    _on_set_request_tags(ctx.get_item("flask_request"), current_span, ctx.get_item("flask_config"))
    block_request_callable = ctx.get_item("block_request_callable")
    if asm_config._asm_enabled:
        set_block_request_callable(functools.partial(block_request_callable, current_span))
        if get_blocked():
            block_request()


def _set_headers_and_response(response, headers, *_):
    if not asm_config._asm_enabled:
        return

    if asm_config._api_security_feature_active:
        if headers:
            # start_response was not called yet, set the HTTP response headers earlier
            if isinstance(headers, dict):
                list_headers = list(headers.items())
            else:
                list_headers = list(headers)
            set_headers_response(list_headers)
        if response and asm_config._api_security_parse_response_body:
            set_body_response(response)


def _call_waf_first(integration, *_):
    if not asm_config._asm_enabled:
        return

    log.debug("%s WAF call for Suspicious Request Blocking on request", integration)
    result = call_waf_callback()
    return result.derivatives if result is not None else None


def _call_waf(integration, *_):
    if not asm_config._asm_enabled:
        return

    log.debug("%s WAF call for Suspicious Request Blocking on response", integration)
    result = call_waf_callback()
    return result.derivatives if result is not None else None


def _on_block_decided(callback):
    if not asm_config._asm_enabled:
        return

    set_value(_CALLBACKS, "flask_block", callback)
    core.on("flask.block.request.content", callback, "block_requested")


def _get_headers_if_appsec():
    if asm_config._asm_enabled:
        return get_headers()


def asm_listen():
    from ddtrace.appsec._handlers import listen

    listen()

    core.on("flask.finalize_request.post", _set_headers_and_response)
    core.on("flask.wrapped_view", _on_wrapped_view, "callback_and_args")
    core.on("flask._patched_request", _on_pre_tracedrequest)
    core.on("wsgi.block_decided", _on_block_decided)
    core.on("flask.start_response", _call_waf_first, "waf")

    core.on("django.start_response.post", _call_waf_first)
    core.on("django.finalize_response", _call_waf)
    core.on("django.after_request_headers", _get_headers_if_appsec, "headers")
    core.on("django.extract_body", _get_headers_if_appsec, "headers")
    core.on("django.after_request_headers.finalize", _set_headers_and_response)
    core.on("flask.set_request_tags", _on_set_request_tags)

    core.on("asgi.start_request", _call_waf_first)
    core.on("asgi.start_response", _call_waf)
    core.on("asgi.finalize_response", _set_headers_and_response)

    core.on("asm.set_blocked", set_blocked)
    core.on("asm.get_blocked", get_blocked, "block_config")

    core.on("context.ended.wsgi.__call__", _on_context_ended)
    core.on("context.ended.asgi.__call__", _on_context_ended)

    core.on("context.ended.django.traced_get_response", _on_context_ended)
    core.on("django.traced_get_response.pre", set_block_request_callable)
