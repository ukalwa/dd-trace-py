import aiohttp_jinja2

from ddtrace import Pin
from ddtrace import config
from ddtrace.internal.constants import COMPONENT
from ddtrace.internal.utils.deprecations import DDTraceDeprecationWarning
from ddtrace.vendor.debtcollector import deprecate

from ...ext import SpanTypes
from ...internal.utils import get_argument_value
from ..trace_utils import unwrap
from ..trace_utils import with_traced_module
from ..trace_utils import wrap


config._add(
    "aiohttp_jinja2",
    dict(),
)


def _get_version():
    # type: () -> str
    return getattr(aiohttp_jinja2, "__version__", "")


def get_version():
    deprecate(
        "get_version is deprecated",
        message="get_version is deprecated",
        removal_version="3.0.0",
        category=DDTraceDeprecationWarning,
    )
    return _get_version()


@with_traced_module
def _traced_render_template(aiohttp_jinja2, pin, func, instance, args, kwargs):
    # original signature:
    # render_template(template_name, request, context, *, app_key=APP_KEY, encoding='utf-8')
    template_name = get_argument_value(args, kwargs, 0, "template_name")
    request = get_argument_value(args, kwargs, 1, "request")
    get_env_kwargs = {}
    if "app_key" in kwargs:
        get_env_kwargs["app_key"] = kwargs["app_key"]
    env = aiohttp_jinja2.get_env(request.app, **get_env_kwargs)

    # the prefix is available only on PackageLoader
    template_prefix = getattr(env.loader, "package_path", "")
    template_meta = "%s/%s" % (template_prefix, template_name)

    with pin.tracer.trace("aiohttp.template", span_type=SpanTypes.TEMPLATE) as span:
        span.set_tag_str(COMPONENT, config.aiohttp_jinja2.integration_name)

        span.set_tag_str("aiohttp.template", template_meta)
        return func(*args, **kwargs)


def traced_render_template(aiohttp_jinja2, pin, func, instance, args, kwargs):
    deprecate(
        "traced_render_template is deprecated",
        message="traced_render_template is deprecated",
        removal_version="3.0.0",
        category=DDTraceDeprecationWarning,
    )
    return _traced_render_template(aiohttp_jinja2, pin, func, instance, args, kwargs)


def _patch(aiohttp_jinja2):
    Pin().onto(aiohttp_jinja2)
    wrap("aiohttp_jinja2", "render_template", _traced_render_template(aiohttp_jinja2))


def patch():
    import aiohttp_jinja2

    if getattr(aiohttp_jinja2, "_datadog_patch", False):
        return

    _patch(aiohttp_jinja2)

    aiohttp_jinja2._datadog_patch = True


def _unpatch(aiohttp_jinja2):
    unwrap(aiohttp_jinja2, "render_template")


def unpatch():
    import aiohttp_jinja2

    if not getattr(aiohttp_jinja2, "_datadog_patch", False):
        return

    _unpatch(aiohttp_jinja2)

    aiohttp_jinja2._datadog_patch = False
