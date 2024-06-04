from collections import defaultdict
import sys
from types import ModuleType
from typing import Any
from typing import Callable

from ddtrace import config

from .event_hub import on


_imported_modules = set(sys.modules.keys())
_import_hooks = defaultdict(set)

ImportHookType = Callable[[str, ModuleType], Any]


def _call_hooks() -> None:
    global _imported_modules

    # Compute the diff of the modules since the last time we checked
    latest_modules = set(sys.modules.keys())
    new_modules = latest_modules - _imported_modules
    _imported_modules.update(new_modules)

    # Call hooks for any newly imported modules
    for module_name in new_modules:
        if module_name not in _import_hooks:
            continue

        module = sys.modules.get(module_name)
        if module is not None:
            for hook in _import_hooks[module_name]:
                hook(module_name, module)


def _on_exec(_: tuple) -> None:
    _call_hooks()


# Register our listener for the sys.audit "exec" event
on("exec", _on_exec)


def on_import(module_name: str) -> Callable[[ImportHookType], None]:
    def _wrapper(hook: ImportHookType) -> None:
        global _import_hooks
        _import_hooks[module_name].add(hook)

        # If the module has already been imported, fire the hook immediately
        if module_name in sys.modules:
            module = sys.modules[module_name]
            hook(module_name, module)

    return _wrapper


# Backwards compatibility for Python < 3.8
if sys.version_info < (3, 8):
    import importlib._bootstrap

    from ddtrace.internal.wrapping import wrap

    def _load_wrapper(original, args, kwargs) -> ModuleType:
        try:
            return original(*args, **kwargs)
        finally:
            try:
                _call_hooks()
            except Exception:
                if config._raise:
                    raise

    wrap(importlib._bootstrap._load, _load_wrapper)
