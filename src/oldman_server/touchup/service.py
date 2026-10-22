from inspect import getmembers, getmodule
from typing import Callable

from .schemes import BaseScheme


class TouchUp:
    _registry: set[tuple[type, str]] = set()
    _originals: dict[tuple[type, str], Callable] = {}

    @classmethod
    def run(cls, app):
        for target, method_name in cls._registry:
            # Rebuild from source after a failed startup; compiled methods are
            # not valid input to inspect.getsource on a subsequent startup.
            method = cls._originals.setdefault(
                (target, method_name), getattr(target, method_name)
            )

            if app.test_mode:
                placeholder = f"_{method_name}"
                if hasattr(target, placeholder):
                    method = getattr(target, placeholder)
                else:
                    setattr(target, placeholder, method)

            module = getmodule(target)
            module_globals = dict(getmembers(module))
            modified = BaseScheme.build(method, module_globals, app)
            setattr(target, method_name, modified)

            target.__touched__ = True

    @classmethod
    def register(cls, target, method_name):
        cls._registry.add((target, method_name))
