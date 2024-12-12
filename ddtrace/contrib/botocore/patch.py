from ddtrace.contrib.internal.botocore.patch import *  # noqa: F403
from ddtrace.internal.utils.deprecations import DDTraceDeprecationWarning
from ddtrace.vendor.debtcollector import deprecate


def __getattr__(name):
    deprecate( # CLEAN UP
        ("%s.%s is deprecated" % (__name__, name)),
        category=DDTraceDeprecationWarning,
    )

    if name in globals():
        return globals()[name]
    raise AttributeError("%s has no attribute %s", __name__, name)
