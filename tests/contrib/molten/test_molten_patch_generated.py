# This test script was automatically generated by the contrib-patch-tests.py
# script. If you want to make changes to it, you should make sure that you have
# removed the ``_generated`` suffix from the file name, to prevent the content
# from being overwritten by future re-generations.

from ddtrace.contrib.molten.patch import patch


try:
    from ddtrace.contrib.molten.patch import unpatch
except ImportError:
    unpatch = None
from tests.contrib.patch import PatchTestCase


class TestMoltenPatch(PatchTestCase.Base):
    __integration_name__ = "molten"
    __module_name__ = "molten"
    __patch_func__ = patch
    __unpatch_func__ = unpatch

    def assert_module_patched(self, molten):
        pass

    def assert_not_module_patched(self, molten):
        pass

    def assert_not_module_double_patched(self, molten):
        pass
