from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from collections.abc import Sequence
from inspect import getmembers, isclass, isdatadescriptor
from os import environ
from pathlib import Path
from typing import Any, Callable, Literal
from warnings import filterwarnings

from oldman_server.errorpages import DEFAULT_FORMAT, check_error_format
from oldman_server.helpers import Default, _default
from oldman_server.http import Http
from oldman_server.log import error_logger
from oldman_server.utils import load_module_from_file_location, str_to_bool


FilterWarningType = (
    Literal["default"]
    | Literal["error"]
    | Literal["ignore"]
    | Literal["always"]
    | Literal["module"]
    | Literal["once"]
)

ENV_PREFIX = "OLDMAN_SERVER_"


DEFAULT_CONFIG = {
    "_FALLBACK_ERROR_FORMAT": _default,
    "ACCESS_LOG": False,
    "AUTO_RELOAD": False,
    # CORS (oldman_server.cors), off while CORS_ORIGINS is not set: the
    # application's default policy; routes may set their own (cors=).
    "CORS_ALLOW_CREDENTIALS": False,
    "CORS_ALLOW_HEADERS": None,  # None: a preflight's requested headers are echoed
    "CORS_ALLOW_METHODS": "GET, HEAD, POST, PUT, PATCH, DELETE",
    "CORS_EXPOSE_HEADERS": None,
    "CORS_MAX_AGE": 600,
    "CORS_ORIGINS": None,  # "*", or origins: "https://a.example, https://*.b.example"
    "EVENT_AUTOREGISTER": False,
    "DEPRECATION_FILTER": "once",
    "FORWARDED_FOR_HEADER": "X-Forwarded-For",
    "FORWARDED_SECRET": None,  # nosec B105
    "GRACEFUL_SHUTDOWN_TIMEOUT": 15.0,
    "GRACEFUL_TCP_CLOSE_TIMEOUT": 5.0,
    # HEAD answered by a path's GET route, OPTIONS by 204 and Allow, where no
    # route has the method (Router._automatic).
    "HTTP_AUTO_HEAD": True,
    "HTTP_AUTO_OPTIONS": True,
    "KEEP_ALIVE_TIMEOUT": 120,
    "KEEP_ALIVE": True,
    "LOG_EXTRA": _default,
    # The native host (docs/en/configuration.md): NATIVE_SERVER None serves through it
    # on Linux when the extension is installed (oldman_server.native).
    "NATIVE_MAX_MEMORY": None,  # from REQUEST_MAX_SIZE and the shard count
    "NATIVE_GC_THRESHOLD": 10000,  # the collector's first threshold while serving
    "NATIVE_MAX_REQUESTS": 4096,
    "NATIVE_MAX_RESPONSE_SIZE": 128 * 1024 * 1024,
    "NATIVE_REACTOR_BACKEND": "epoll",
    "NATIVE_SAME_THREAD": None,  # chosen by the CPUs per worker (oldman_server.native.cpus)
    "NATIVE_SERVER": None,
    "NATIVE_SHARD_CPUS": None,
    "NATIVE_SHARDS": None,  # up to 2
    "NATIVE_WORKER_CPUS": None,
    "NATIVE_WORKER_LISTEN": None,  # e.g. "/run/app/{n}.sock" (oldman_server.native.listen)
    "NO_COLOR": False,
    "NOISY_EXCEPTIONS": False,
    # Libraries for LD_PRELOAD (e.g. an allocator): app.run() runs the program
    # again with them when they are missing (Linux).
    "PRELOAD": None,
    "PROXIES_COUNT": None,
    "REAL_IP_HEADER": None,
    "TRUSTED_PROXIES": None,
    "REQUEST_BUFFER_SIZE": 65536,
    "REQUEST_MAX_HEADER_SIZE": 8192,  # Cannot exceed 16384
    "REQUEST_ID_HEADER": "X-Request-ID",
    "REQUEST_MAX_SIZE": 100_000_000,
    "REQUEST_TIMEOUT": 60,
    "RESPONSE_TIMEOUT": 60,
    "TOUCHUP": _default,
    # Permission bits of the unix sockets the server binds (unix=,
    # NATIVE_WORKER_LISTEN): who may connect. 0o666, as Sanic, lets every local
    # user connect, whom TRUSTED_PROXIES="unix" then trusts.
    "UNIX_SOCKET_MODE": 0o666,
    "USE_UVLOOP": _default,
    "WEBSOCKET_MAX_SIZE": 2**20,  # 1 MiB
    "WEBSOCKET_PING_INTERVAL": 20,
    "WEBSOCKET_PING_TIMEOUT": 20,
    # Seconds a server worker may go without a heartbeat from its event loop
    # before the worker manager restarts it (oldman_server.worker.health);
    # None or 0: not watched.
    "WORKER_HEALTH_TIMEOUT": None,
}


class DescriptorMeta(ABCMeta):
    """Metaclass for Config: the settings with a property setter, and the
    default settings a subclass gives as upper-case class attributes."""

    def __init__(cls, *_):
        # An upper-case class attribute that is not a property or method is a
        # default setting, as update_config(a class) reads one. It moves into
        # __class_defaults__ (the bases' first, the subclass's over them) and
        # off the class: left there it would shadow the dict for attribute
        # reads, so the name had one value for config.X and another for
        # config.get("X"), which is how the native host reads its settings.
        own = {
            name: value
            for name, value in vars(cls).items()
            if name.isupper() and not hasattr(value, "__get__")
        }
        for name in own:
            delattr(cls, name)
        inherited: dict[str, Any] = {}
        for base in reversed(cls.__mro__[1:]):
            inherited.update(vars(base).get("__class_defaults__", {}))
        cls.__class_defaults__ = {**inherited, **own}
        cls.__setters__ = {name for name, _ in getmembers(cls, cls._is_setter)}

    @staticmethod
    def _is_setter(member: object):
        return isdatadescriptor(member) and hasattr(member, "setter")


class DetailedConverter(ABC):
    """Base class for detailed converters that need additional context.

    DetailedConverter provides access to the full environment variable key,
    the raw value, and the current config defaults. This allows for more
    sophisticated conversion logic that can take into account the variable
    name pattern, perform validation, or use default values for fallback.

    Examples:
        ```python
        # Example of a converter that casts values to the type of the default
        class DefaultsCastConverter(DetailedConverter):
            def __call__(self, full_key: str, config_key: str, value: str,
                                                        defaults: dict) -> Any:
                try:
                    if config_key in defaults:
                        return type(defaults[config_key])(value)
                except (ValueError, TypeError):
                    raise ValueError
        ```
    """

    @abstractmethod
    def __call__(
        self, full_key: str, config_key: str, value: str, defaults: dict
    ) -> Any:
        """Convert an environment variable to a Python value.

        Args:
            full_key: The full environment variable name (with prefix)
                            (e.g., "OLDMAN_SERVER_DATABASE_URL")
            config_key: The environment variable name (without prefix)
                            (e.g., "DATABASE_URL")
            value: The raw string value from the environment
            defaults: The current default configuration values

        Returns:
            The converted Python value

        Raises:
            ValueError: If the value cannot be converted by this converter
        """


class Config(dict, metaclass=DescriptorMeta):
    """Configuration object for Sanic.

    You can use this object to both: (1) configure how Sanic will operate, and
    (2) manage your application's custom configuration values.
    """

    ACCESS_LOG: bool
    AUTO_RELOAD: bool
    CORS_ALLOW_CREDENTIALS: bool
    CORS_ALLOW_HEADERS: str | Sequence[str] | None
    CORS_ALLOW_METHODS: str | Sequence[str]
    CORS_EXPOSE_HEADERS: str | Sequence[str] | None
    CORS_MAX_AGE: int
    CORS_ORIGINS: str | Sequence[str] | None
    EVENT_AUTOREGISTER: bool
    DEPRECATION_FILTER: FilterWarningType
    FORWARDED_FOR_HEADER: str
    FORWARDED_SECRET: str | None
    GRACEFUL_SHUTDOWN_TIMEOUT: float
    GRACEFUL_TCP_CLOSE_TIMEOUT: float
    HTTP_AUTO_HEAD: bool
    HTTP_AUTO_OPTIONS: bool
    KEEP_ALIVE_TIMEOUT: int
    KEEP_ALIVE: bool
    LOG_EXTRA: Default | bool
    NATIVE_GC_THRESHOLD: int | None
    NATIVE_MAX_MEMORY: int | None
    NATIVE_MAX_REQUESTS: int
    NATIVE_MAX_RESPONSE_SIZE: int
    NATIVE_REACTOR_BACKEND: str
    NATIVE_SAME_THREAD: bool | None
    NATIVE_SERVER: bool | None
    NATIVE_SHARD_CPUS: str | int | Sequence[int] | None
    NATIVE_SHARDS: int | None
    NATIVE_WORKER_CPUS: str | None
    NATIVE_WORKER_LISTEN: str | None
    NO_COLOR: bool
    NOISY_EXCEPTIONS: bool
    PRELOAD: str | None
    PROXIES_COUNT: int | None
    REAL_IP_HEADER: str | None
    TRUSTED_PROXIES: str | Sequence[str] | None
    UNIX_SOCKET_MODE: int | str
    REQUEST_BUFFER_SIZE: int
    REQUEST_MAX_HEADER_SIZE: int
    REQUEST_ID_HEADER: str
    REQUEST_MAX_SIZE: int
    REQUEST_TIMEOUT: int
    RESPONSE_TIMEOUT: int
    SERVER_NAME: str
    TOUCHUP: Default | bool
    USE_UVLOOP: Default | bool
    WEBSOCKET_MAX_SIZE: int
    WEBSOCKET_PING_INTERVAL: int
    WEBSOCKET_PING_TIMEOUT: int
    WORKER_HEALTH_TIMEOUT: int | float | None

    def __init__(
        self,
        defaults: dict[str, str | bool | int | float | None] | None = None,
        env_prefix: str | None = ENV_PREFIX,
        keep_alive: bool | None = None,
        *,
        converters: Sequence[Callable[[str], Any]] | None = None,
    ):
        defaults = defaults or {}
        class_defaults = type(self).__class_defaults__
        self.defaults = {**DEFAULT_CONFIG, **class_defaults, **defaults}
        super().__init__(DEFAULT_CONFIG)
        # Through update(), so a setting with a property (FALLBACK_ERROR_FORMAT)
        # takes its value through the setter; the environment overrides both.
        self.update(class_defaults, defaults)
        self._configure_warnings()

        self._converters = [str, str_to_bool, float, int]

        if converters:
            for converter in converters:
                self.register_type(converter)

        if keep_alive is not None:
            self.KEEP_ALIVE = keep_alive

        if env_prefix != ENV_PREFIX:
            if env_prefix:
                self.load_environment_vars(env_prefix)
        else:
            self.load_environment_vars(ENV_PREFIX)

        self._configure_header_size()
        self._check_error_format()
        self._init = True

    def __getattr__(self, attr: Any):
        try:
            return self[attr]
        except KeyError as ke:
            raise AttributeError(f"Config has no '{ke.args[0]}'")

    def __setattr__(self, attr: str, value: Any) -> None:
        self.update({attr: value})

    def __setitem__(self, attr: str, value: Any) -> None:
        self.update({attr: value})

    def update(self, *other: Any, **kwargs: Any) -> None:
        """Update the config with new values.

        This method will update the config with the values from the provided
        `other` objects, and then update the config with the provided
        `kwargs`. The `other` objects can be any object that can be converted
        to a dictionary, such as a `dict`, `Config` object, or `str` path to a
        Python file. The `kwargs` must be a dictionary of key-value pairs.

        .. note::
            Only upper case settings are considered

        Args:
            *other: Any number of objects that can be converted to a
                dictionary.
            **kwargs: Any number of key-value pairs.

        Raises:
            AttributeError: If a key is not in the config.

        Examples:
            ```python
            config.update(
                {"A": 1, "B": 2},
                {"C": 3, "D": 4},
                E=5,
                F=6,
            )
            ```
        """
        kwargs.update({k: v for item in other for k, v in dict(item).items()})
        setters: dict[str, Any] = {
            k: kwargs.pop(k)
            for k in {**kwargs}.keys()
            if k in self.__class__.__setters__
        }

        for key, value in setters.items():
            try:
                super().__setattr__(key, value)
            except AttributeError:
                ...

        super().update(**kwargs)
        for attr, value in {**setters, **kwargs}.items():
            self._post_set(attr, value)

    def _post_set(self, attr, value) -> None:
        if self.get("_init"):
            if attr in (
                "REQUEST_MAX_HEADER_SIZE",
                "REQUEST_BUFFER_SIZE",
                "REQUEST_MAX_SIZE",
            ):
                self._configure_header_size()

        if attr == "DEPRECATION_FILTER":
            self._configure_warnings()

    @property
    def FALLBACK_ERROR_FORMAT(self) -> str:
        if isinstance(self._FALLBACK_ERROR_FORMAT, Default):
            return DEFAULT_FORMAT
        return self._FALLBACK_ERROR_FORMAT

    @FALLBACK_ERROR_FORMAT.setter
    def FALLBACK_ERROR_FORMAT(self, value):
        self._check_error_format(value)
        if (
            not isinstance(self._FALLBACK_ERROR_FORMAT, Default)
            and value != self._FALLBACK_ERROR_FORMAT
        ):
            error_logger.warning(
                "Setting config.FALLBACK_ERROR_FORMAT on an already "
                "configured value may have unintended consequences."
            )
        self._FALLBACK_ERROR_FORMAT = value

    def _configure_header_size(self):
        Http.set_header_max_size(
            self.REQUEST_MAX_HEADER_SIZE,
            self.REQUEST_BUFFER_SIZE - 4096,
            self.REQUEST_MAX_SIZE,
        )

    def _configure_warnings(self):
        filterwarnings(
            self.DEPRECATION_FILTER,
            category=DeprecationWarning,
            module=r"oldman_server.*",
        )

    def _check_error_format(self, format: str | None = None):
        check_error_format(format or self.FALLBACK_ERROR_FORMAT)

    def load_environment_vars(self, prefix=ENV_PREFIX):
        """Load environment variables into the config.

        Looks for prefixed environment variables and applies them to the
        configuration if present. This is called automatically when Sanic
        starts up to load environment variables into config. Environment
        variables should start with the defined prefix and should only
        contain uppercase letters.

        It will automatically hydrate the following types:

        - ``int``
        - ``float``
        - ``bool``

        Anything else will be imported as a ``str``. If you would like to add
        additional types to this list, you can use
        :meth:`oldman_server.config.Config.register_type`. Just make sure that they
        are registered before you instantiate your application.

        You likely won't need to call this method directly.

        See [Configuration](/en/guide/deployment/configuration) for more details.

        Args:
            prefix (str): The prefix to use when looking for environment
                variables. Defaults to `OLDMAN_SERVER_`.


        Examples:
            ```python
            # Environment variables
            # OLDMAN_SERVER_SERVER_NAME=example.com
            # OLDMAN_SERVER_SERVER_PORT=9999
            # OLDMAN_SERVER_SERVER_AUTORELOAD=true

            # Python
            app.config.load_environment_vars()
            ```
        """  # noqa: E501
        for key, value in environ.items():
            if not key.startswith(prefix) or not key.isupper():
                continue

            _, config_key = key.split(prefix, 1)

            for converter in reversed(self._converters):
                try:
                    if isinstance(converter, DetailedConverter):
                        self[config_key] = converter(
                            key, config_key, value, self.defaults
                        )
                    else:
                        self[config_key] = converter(value)
                    break
                except ValueError:
                    pass

    def update_config(self, config: bytes | str | dict[str, Any] | Any):
        """Update app.config.

        .. note::

            Only upper case settings are considered

        See [Configuration](/en/guide/deployment/configuration) for more details.

        Args:
            config (Union[bytes, str, dict, Any]): Path to py file holding
                settings, dict holding settings, or any object holding
                settings.

        Examples:
            You can upload app config by providing path to py file
            holding settings.

            ```python
            # /some/py/file
            A = 1
            B = 2
            ```

            ```python
            config.update_config("${some}/py/file")
            ```

            Yes you can put environment variable here, but they must be provided
            in format: ``${some_env_var}``, and mark that ``$some_env_var`` is
            treated as plain string.

            You can upload app config by providing dict holding settings.

            ```python
            d = {"A": 1, "B": 2}
            config.update_config(d)
            ```

            You can upload app config by providing any object holding settings,
            but in such case config.__dict__ will be used as dict holding settings.

            ```python
            class C:
                A = 1
                B = 2

            config.update_config(C)
            ```
        """  # noqa: E501
        if isinstance(config, (bytes, str, Path)):
            config = load_module_from_file_location(location=config)

        if not isinstance(config, dict):
            cfg = {}
            if not isclass(config):
                cfg.update(
                    {
                        key: getattr(config, key)
                        for key in config.__class__.__dict__.keys()
                    }
                )

            config = dict(config.__dict__)
            config.update(cfg)

        config = dict(filter(lambda i: i[0].isupper(), config.items()))

        self.update(config)

    load = update_config

    def register_type(self, converter: Callable[[str], Any]) -> None:
        """Register a custom type converter.

        Allows for adding custom function to cast from a string value to any
        other type. The function should raise ValueError if it is not the
        correct type.

        Args:
            converter (Callable[[str], Any]): A function that takes a string
            and returns a value of any type, or a DetailedConverter instance.

        Examples:
            ```python
            def my_converter(value: str) -> Any:
                # Do something to convert the value
                return value

            config.register_type(my_converter)

            # Or use a DetailedConverter for more context
            # Example of a converter that casts values to
            # the type of the default
            class DefaultsCastConverter(DetailedConverter):
                def __call__(self, full_key: str, config_key: str, value: str,
                                                    defaults: dict) -> Any:
                    try:
                        if config_key in defaults:
                            return type(defaults[config_key])(value)
                    except (ValueError, TypeError):
                        raise ValueError

            config.register_type(DefaultsCastConverter())
            ```
        """
        if converter in self._converters:
            error_logger.warning(
                f"Configuration value converter '{converter.__name__}' has "
                "already been registered"
            )
            return
        self._converters.append(converter)
