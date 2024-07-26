# 3p
import aiopg.connection
import psycopg2.extensions

from ddtrace import config
from ddtrace.contrib.aiopg.connection import AIOTracedConnection
from ddtrace.contrib.psycopg.connection import patch_conn as psycopg_patch_conn
from ddtrace.contrib.psycopg.extensions import _patch_extensions
from ddtrace.contrib.psycopg.extensions import _unpatch_extensions
from ddtrace.internal.utils.deprecations import DDTraceDeprecationWarning
from ddtrace.internal.utils.wrappers import unwrap as _u
from ddtrace.vendor import wrapt
from ddtrace.vendor.debtcollector import deprecate

from ...internal.schema import schematize_service_name


config._add(
    "aiopg",
    dict(
        _default_service=schematize_service_name("postgres"),
    ),
)


def _get_version():
    # type: () -> str
    return getattr(aiopg, "__version__", "")


def get_version():
    deprecate(
        "get_version is deprecated",
        message="get_version is deprecated",
        removal_version="3.0.0",
        category=DDTraceDeprecationWarning,
    )
    return _get_version()


def patch():
    """Patch monkey patches psycopg's connection function
    so that the connection's functions are traced.
    """
    if getattr(aiopg, "_datadog_patch", False):
        return
    aiopg._datadog_patch = True

    wrapt.wrap_function_wrapper(aiopg.connection, "connect", _patched_connect)
    _patch_extensions(_aiopg_extensions)  # do this early just in case


def unpatch():
    if getattr(aiopg, "_datadog_patch", False):
        aiopg._datadog_patch = False
        _u(aiopg.connection, "connect")
        _unpatch_extensions(_aiopg_extensions)


async def _patched_connect(connect_func, _, args, kwargs):
    conn = await connect_func(*args, **kwargs)
    return psycopg_patch_conn(conn, traced_conn_cls=AIOTracedConnection)


async def patched_connect(connect_func, _, args, kwargs):
    deprecate(
        "patched_connect is deprecated",
        message="patched_connect is deprecated",
        removal_version="3.0.0",
        category=DDTraceDeprecationWarning,
    )
    return _patched_connect(connect_func, _, args, kwargs)


def _extensions_register_type(func, _, args, kwargs):
    def _unroll_args(obj, scope=None):
        return obj, scope

    obj, scope = _unroll_args(*args, **kwargs)

    # register_type performs a c-level check of the object
    # type so we must be sure to pass in the actual db connection
    if scope and isinstance(scope, wrapt.ObjectProxy):
        scope = scope.__wrapped__._conn

    return func(obj, scope) if scope else func(obj)


# extension hooks
_aiopg_extensions = [
    (psycopg2.extensions.register_type, psycopg2.extensions, "register_type", _extensions_register_type),
]
