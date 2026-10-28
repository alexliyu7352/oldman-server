import re

from typing import Any

from oldman_server.base.meta import AppMeta
from oldman_server.exceptions import OldmanServerException
from oldman_server.mixins.exceptions import ExceptionMixin
from oldman_server.mixins.listeners import ListenerMixin
from oldman_server.mixins.middleware import MiddlewareMixin
from oldman_server.mixins.routes import RouteMixin
from oldman_server.mixins.signals import SignalMixin
from oldman_server.mixins.static import StaticMixin


VALID_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_\-]*$")


class BaseApp(
    RouteMixin,
    StaticMixin,
    MiddlewareMixin,
    ListenerMixin,
    ExceptionMixin,
    SignalMixin,
    metaclass=AppMeta,
):
    __slots__ = ("name",)

    def __init__(
        self, name: str | None = None, *args: Any, **kwargs: Any
    ) -> None:
        class_name = self.__class__.__name__

        if name is None:
            raise OldmanServerException(
                f"{class_name} instance cannot be unnamed. "
                "Please use OldmanServer(name='your_application_name') instead.",
            )

        if not VALID_NAME.match(name):
            raise OldmanServerException(
                f"{class_name} instance named '{name}' uses an invalid "
                "format. Names must start with an ASCII letter or _ and "
                "contain only ASCII letters, digits, _ and -."
            )

        self.name = name

        for base in BaseApp.__bases__:
            base.__init__(self, *args, **kwargs)  # type: ignore

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} {self.name}>"

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(name="{self.name}")'

    def __setattr__(self, name: str, value: Any) -> None:
        try:
            super().__setattr__(name, value)
        except AttributeError as e:
            raise AttributeError(
                f"Setting variables on {self.__class__.__name__} instances is "
                "not allowed. You should change your "
                f"{self.__class__.__name__} instance to use "
                f"instance.ctx.{name} instead.",
            ) from e
