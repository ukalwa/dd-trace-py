#!/usr/bin/env python3

from ddtrace.appsec._iast._taint_tracking import taint_pyobject, OriginType, get_tainted_ranges, is_pyobject_tainted
from ddtrace.appsec._iast._taint_tracking.aspects import stringio_aspect
from ddtrace.appsec._common_module_patches import patch_common_modules

from tests.utils import override_global_config


def test_stringio_aspect_read():
    with override_global_config(dict(_iast_enabled=True)):
        patch_common_modules()
        tainted = taint_pyobject(
            pyobject="foobar",
            source_name="test_stringio_read_aspect_tainted_string",
            source_value="foobar",
            source_origin=OriginType.PARAMETER,
        )
        sio = stringio_aspect(None, 0, tainted)
        val = sio.read()  # devuelve rango
        assert is_pyobject_tainted(val)
        ranges = get_tainted_ranges(val)  # devuelve rango
        assert len(ranges) == 1
        print("JJJ ranges: %s" % str(ranges))
