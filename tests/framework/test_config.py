import logging

from contextlib import contextmanager
from os import environ
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from unittest.mock import Mock, call

import pytest

from pytest import MonkeyPatch

from oldman_server import OldmanServer
from oldman_server.config import (
    DEFAULT_CONFIG,
    ENV_PREFIX,
    Config,
    DetailedConverter,
)
from oldman_server.exceptions import PyFileError

from .conftest import get_port


# Settings the test run itself gives every app through the environment (the
# native run: OLDMAN_SERVER_NATIVE_SERVER, OLDMAN_SERVER_NATIVE_SHARDS); the
# DEFAULT_CONFIG comparisons below leave them out.
SET_BY_THE_RUN = {
    key[len(ENV_PREFIX):] for key in environ if key.startswith(ENV_PREFIX)
}


@contextmanager
def temp_path():
    """a simple cross platform replacement for NamedTemporaryFile"""
    with TemporaryDirectory() as td:
        yield Path(td, "file")


class ConfigTest:
    not_for_config = "should not be used"
    CONFIG_VALUE = "should be used"

    @property
    def ANOTHER_VALUE(self):
        return self.CONFIG_VALUE

    @property
    def another_not_for_config(self):
        return self.not_for_config


class UltimateAnswer:
    def __init__(self, answer):
        self.answer = int(answer)


def test_load_from_object(app: OldmanServer):
    app.config.load(ConfigTest)
    assert "CONFIG_VALUE" in app.config
    assert app.config.CONFIG_VALUE == "should be used"
    assert "not_for_config" not in app.config


def test_load_from_object_string(app: OldmanServer):
    app.config.load("tests.framework.test_config.ConfigTest")
    assert "CONFIG_VALUE" in app.config
    assert app.config.CONFIG_VALUE == "should be used"
    assert "not_for_config" not in app.config


def test_load_from_instance(app: OldmanServer):
    app.config.load(ConfigTest())
    assert "CONFIG_VALUE" in app.config
    assert app.config.CONFIG_VALUE == "should be used"
    assert app.config.ANOTHER_VALUE == "should be used"
    assert "not_for_config" not in app.config
    assert "another_not_for_config" not in app.config


def test_load_from_object_string_exception(app: OldmanServer):
    with pytest.raises(ImportError):
        app.config.load("test_config.Config.test")


def test_auto_env_prefix():
    environ["OLDMAN_SERVER_TEST_ANSWER"] = "42"
    app = OldmanServer(name="Test")
    assert app.config.TEST_ANSWER == 42
    del environ["OLDMAN_SERVER_TEST_ANSWER"]


def test_auto_bool_env_prefix():
    environ["OLDMAN_SERVER_TEST_ANSWER"] = "True"
    app = OldmanServer(name="Test")
    assert app.config.TEST_ANSWER is True
    del environ["OLDMAN_SERVER_TEST_ANSWER"]


@pytest.mark.parametrize("env_prefix", [None, ""])
def test_empty_load_env_prefix(env_prefix):
    environ["OLDMAN_SERVER_TEST_ANSWER"] = "42"
    app = OldmanServer(name="Test", env_prefix=env_prefix)
    assert getattr(app.config, "TEST_ANSWER", None) is None
    del environ["OLDMAN_SERVER_TEST_ANSWER"]


def test_env_prefix():
    environ["MYAPP_TEST_ANSWER"] = "42"
    app = OldmanServer(name="Test", env_prefix="MYAPP_")
    assert app.config.TEST_ANSWER == 42
    del environ["MYAPP_TEST_ANSWER"]


def test_env_prefix_float_values():
    environ["MYAPP_TEST_ROI"] = "2.3"
    app = OldmanServer(name="Test", env_prefix="MYAPP_")
    assert app.config.TEST_ROI == 2.3
    del environ["MYAPP_TEST_ROI"]


def test_env_prefix_string_value():
    environ["MYAPP_TEST_TOKEN"] = "somerandomtesttoken"
    app = OldmanServer(name="Test", env_prefix="MYAPP_")
    assert app.config.TEST_TOKEN == "somerandomtesttoken"
    del environ["MYAPP_TEST_TOKEN"]


def test_env_w_custom_converter():
    environ["OLDMAN_SERVER_TEST_ANSWER"] = "42"

    config = Config(converters=[UltimateAnswer])
    app = OldmanServer(name="Test", config=config)
    assert isinstance(app.config.TEST_ANSWER, UltimateAnswer)
    assert app.config.TEST_ANSWER.answer == 42
    del environ["OLDMAN_SERVER_TEST_ANSWER"]


def test_env_lowercase():
    environ["OLDMAN_SERVER_test_answer"] = "42"
    app = OldmanServer(name="Test")
    assert "test_answer" not in app.config
    del environ["OLDMAN_SERVER_test_answer"]


def test_add_converter_multiple_times(caplog):
    def converter(): ...

    message = (
        "Configuration value converter 'converter' has already been registered"
    )
    config = Config()
    config.register_type(converter)
    with caplog.at_level(logging.WARNING):
        config.register_type(converter)

    assert ("oldman_server.error", logging.WARNING, message) in caplog.record_tuples
    assert len(config._converters) == 5


class MockDetailedConverter(DetailedConverter):
    """Mock converter that returns a dict with all passed parameters."""

    def __call__(
        self, full_key: str, config_key: str, value: str, defaults: dict
    ):
        return {
            "full_key": full_key,
            "config_key": config_key,
            "value": value,
            "has_default": config_key in defaults,
            "default_value": defaults.get(config_key, None),
        }


def test_detailed_converter_basic_functionality():
    """Test that DetailedConverter receives all expected parameters."""
    environ["OLDMAN_SERVER_TEST_DETAILED"] = "test_value"

    config = Config(converters=[MockDetailedConverter()])

    result = config.TEST_DETAILED
    assert isinstance(result, dict)
    assert result["full_key"] == "OLDMAN_SERVER_TEST_DETAILED"
    assert result["config_key"] == "TEST_DETAILED"
    assert result["value"] == "test_value"
    assert result["has_default"] is False
    assert result["default_value"] is None

    del environ["OLDMAN_SERVER_TEST_DETAILED"]


def test_detailed_converter_with_defaults():
    """Test DetailedConverter with custom defaults."""
    environ["OLDMAN_SERVER_CUSTOM_VALUE"] = "42"

    defaults = {"CUSTOM_VALUE": 100}
    config = Config(defaults=defaults, converters=[MockDetailedConverter()])

    result = config.CUSTOM_VALUE
    assert isinstance(result, dict)
    assert result["full_key"] == "OLDMAN_SERVER_CUSTOM_VALUE"
    assert result["config_key"] == "CUSTOM_VALUE"
    assert result["value"] == "42"
    assert result["has_default"] is True
    assert result["default_value"] == 100

    del environ["OLDMAN_SERVER_CUSTOM_VALUE"]


def test_detailed_converter_with_custom_prefix():
    """Test DetailedConverter with custom environment prefix."""
    environ["MYAPP_TEST_VALUE"] = "custom_test"

    config = Config(env_prefix="MYAPP_", converters=[MockDetailedConverter()])

    result = config.TEST_VALUE
    assert isinstance(result, dict)
    assert result["full_key"] == "MYAPP_TEST_VALUE"
    assert result["config_key"] == "TEST_VALUE"
    assert result["value"] == "custom_test"

    del environ["MYAPP_TEST_VALUE"]


def test_load_from_file(app: OldmanServer):
    config = dedent(
        """
    VALUE = 'some value'
    condition = 1 == 1
    if condition:
        CONDITIONAL = 'should be set'
    """
    )
    with temp_path() as config_path:
        config_path.write_text(config)
        app.config.load(str(config_path))
        assert "VALUE" in app.config
        assert app.config.VALUE == "some value"
        assert "CONDITIONAL" in app.config
        assert app.config.CONDITIONAL == "should be set"
        assert "condition" not in app.config


def test_load_from_missing_file(app: OldmanServer):
    with pytest.raises(IOError):
        app.config.load("non-existent file")


def test_load_from_envvar(app: OldmanServer):
    config = "VALUE = 'some value'"
    with temp_path() as config_path:
        config_path.write_text(config)
        environ["APP_CONFIG"] = str(config_path)
        app.config.load("${APP_CONFIG}")
        assert "VALUE" in app.config
        assert app.config.VALUE == "some value"


def test_load_from_missing_envvar(app: OldmanServer):
    with pytest.raises(IOError) as e:
        app.config.load("non-existent variable")
        assert str(e.value) == (
            "The environment variable 'non-existent "
            "variable' is not set and thus configuration "
            "could not be loaded."
        )


def test_load_config_from_file_invalid_syntax(app: OldmanServer):
    config = "VALUE = some value"
    with temp_path() as config_path:
        config_path.write_text(config)

        with pytest.raises(PyFileError):
            app.config.load(config_path)


def test_overwrite_exisiting_config(app: OldmanServer):
    app.config.DEFAULT = 1

    class Config:
        DEFAULT = 2

    app.config.load(Config)
    assert app.config.DEFAULT == 2


def test_overwrite_exisiting_config_ignore_lowercase(app: OldmanServer):
    app.config.default = 1

    class Config:
        default = 2

    app.config.load(Config)
    assert app.config.default == 1


def test_missing_config(app: OldmanServer):
    with pytest.raises(AttributeError, match="Config has no 'NON_EXISTENT'"):
        _ = app.config.NON_EXISTENT


def test_config_defaults():
    """
    load DEFAULT_CONFIG
    """
    conf = Config()
    for key, value in DEFAULT_CONFIG.items():
        if key not in SET_BY_THE_RUN:
            assert getattr(conf, key) == value


def test_config_custom_defaults():
    """
    we should have all the variables from defaults rewriting them with
    custom defaults passed in
    Config
    """
    custom_defaults = {
        "REQUEST_MAX_SIZE": 1,
        "KEEP_ALIVE": False,
        "ACCESS_LOG": False,
    }
    conf = Config(defaults=custom_defaults)
    for key, value in DEFAULT_CONFIG.items():
        if key in SET_BY_THE_RUN:
            continue
        if key in custom_defaults.keys():
            value = custom_defaults[key]
        assert getattr(conf, key) == value


def test_config_custom_defaults_with_env():
    """
    test that environment variables has higher priority than DEFAULT_CONFIG
    and passed defaults dict
    """
    custom_defaults = {
        "REQUEST_MAX_SIZE123": 1,
        "KEEP_ALIVE123": False,
        "ACCESS_LOG123": False,
    }

    environ_defaults = {
        "OLDMAN_SERVER_REQUEST_MAX_SIZE123": "2",
        "OLDMAN_SERVER_KEEP_ALIVE123": "True",
        "OLDMAN_SERVER_ACCESS_LOG123": "False",
    }

    for key, value in environ_defaults.items():
        environ[key] = value

    conf = Config(defaults=custom_defaults)
    for key, value in DEFAULT_CONFIG.items():
        if key in SET_BY_THE_RUN:
            continue
        if "OLDMAN_SERVER_" + key in environ_defaults.keys():
            value = environ_defaults["OLDMAN_SERVER_" + key]
            try:
                value = int(value)
            except ValueError:
                if value in ["True", "False"]:
                    value = value == "True"

        assert getattr(conf, key) == value

    for key, value in environ_defaults.items():
        del environ[key]


@pytest.mark.parametrize("access_log", (True, False))
def test_config_access_log_passing_in_run(app: OldmanServer, port, access_log):
    assert app.config.ACCESS_LOG is False

    @app.listener("after_server_start")
    async def _request(sanic, loop):
        app.stop()

    app.run(port=port, access_log=access_log, single_process=True)
    assert app.config.ACCESS_LOG is access_log


@pytest.mark.asyncio
async def test_config_access_log_passing_in_create_server(app: OldmanServer):
    assert app.config.ACCESS_LOG is False

    @app.listener("after_server_start")
    async def _request(sanic, loop):
        app.stop()

    await app.create_server(
        port=get_port(), access_log=False, return_asyncio_server=True
    )
    assert app.config.ACCESS_LOG is False

    await app.create_server(
        port=get_port(), access_log=True, return_asyncio_server=True
    )
    assert app.config.ACCESS_LOG is True


def test_config_rewrite_keep_alive():
    config = Config()
    assert config.KEEP_ALIVE == DEFAULT_CONFIG["KEEP_ALIVE"]
    config = Config(keep_alive=True)
    assert config.KEEP_ALIVE is True
    config = Config(keep_alive=False)
    assert config.KEEP_ALIVE is False

    # use defaults
    config = Config(defaults={"KEEP_ALIVE": False})
    assert config.KEEP_ALIVE is False
    config = Config(defaults={"KEEP_ALIVE": True})
    assert config.KEEP_ALIVE is True


_test_setting_as_dict = {"TEST_SETTING_VALUE": 1}
_test_setting_as_class = type("C", (), {"TEST_SETTING_VALUE": 1})
_test_setting_as_module = str(
    Path(__file__).parent / "static/app_test_config.py"
)


@pytest.mark.parametrize(
    "conf_object",
    [
        _test_setting_as_dict,
        _test_setting_as_class,
        _test_setting_as_module,
    ],
    ids=["from_dict", "from_class", "from_file"],
)
def test_update(app: OldmanServer, conf_object):
    app.update_config(conf_object)
    assert app.config["TEST_SETTING_VALUE"] == 1


def test_update_from_lowercase_key(app: OldmanServer):
    d = {"test_setting_value": 1}
    app.update_config(d)
    assert "test_setting_value" not in app.config


def test_config_set_methods(app: OldmanServer, monkeypatch: MonkeyPatch):
    post_set = Mock()
    monkeypatch.setattr(Config, "_post_set", post_set)

    app.config.FOO = 1
    post_set.assert_called_once_with("FOO", 1)
    post_set.reset_mock()

    app.config["FOO"] = 2
    post_set.assert_called_once_with("FOO", 2)
    post_set.reset_mock()

    app.config.update({"FOO": 3})
    post_set.assert_called_once_with("FOO", 3)
    post_set.reset_mock()

    app.config.update([("FOO", 4)])
    post_set.assert_called_once_with("FOO", 4)
    post_set.reset_mock()

    app.config.update(FOO=5)
    post_set.assert_called_once_with("FOO", 5)
    post_set.reset_mock()

    app.config.update({"FOO": 6}, {"BAR": 7})
    post_set.assert_has_calls(
        calls=[
            call("FOO", 6),
            call("BAR", 7),
        ]
    )
    post_set.reset_mock()

    app.config.update({"FOO": 8}, BAR=9)
    post_set.assert_has_calls(
        calls=[
            call("FOO", 8),
            call("BAR", 9),
        ],
        any_order=True,
    )
    post_set.reset_mock()

    app.config.update_config({"FOO": 10})
    post_set.assert_called_once_with("FOO", 10)


@pytest.mark.parametrize("count", [-1, 1.5, "2", True])
def test_invalid_proxy_count(app: OldmanServer, count):
    app.config.PROXIES_COUNT = count

    with pytest.raises(ValueError, match="PROXIES_COUNT is None or a non-negative integer"):
        app.prepare()


@pytest.mark.parametrize("count", [None, 0, 2])
def test_valid_proxy_count(app: OldmanServer, count):
    from oldman_server.headers import check_proxies_count

    app.config.PROXIES_COUNT = count
    check_proxies_count(app.config)


class SettingsConfig(Config):
    FOO = "class value"
    NATIVE_SHARDS = 1
    FALLBACK_ERROR_FORMAT = "text"
    lowercase = "not a setting"

    def method(self):
        return "a method stays"


class MoreSettings(SettingsConfig):
    FOO = "grandchild value"
    BAR = 2


def test_class_attributes_of_a_subclass_are_settings():
    config = SettingsConfig()
    assert config.FOO == config["FOO"] == config.get("FOO") == "class value"
    assert "FOO" in config and dict(config)["FOO"] == "class value"
    assert config.get("NATIVE_SHARDS") == 1  # how the native host reads it
    assert config.lowercase == "not a setting" and "lowercase" not in config
    assert config.method() == "a method stays"


def test_a_class_attribute_setting_can_be_changed():
    config = SettingsConfig()
    config.FOO = "assigned"
    assert config.FOO == config.get("FOO") == "assigned"
    assert config.FALLBACK_ERROR_FORMAT == "text"
    config.FALLBACK_ERROR_FORMAT = "json"
    assert config.FALLBACK_ERROR_FORMAT == "json"


def test_subclass_settings_override_their_bases():
    config = MoreSettings()
    assert config.FOO == "grandchild value"
    assert config.BAR == 2
    assert config.get("NATIVE_SHARDS") == 1


def test_settings_given_as_defaults_go_through_their_setter():
    config = Config(defaults={"FALLBACK_ERROR_FORMAT": "text"})
    assert config.FALLBACK_ERROR_FORMAT == "text"


def test_the_environment_overrides_a_class_attribute_setting(monkeypatch):
    monkeypatch.setenv("OLDMAN_SERVER_FOO", "from the environment")
    assert SettingsConfig().FOO == "from the environment"


def test_an_app_takes_class_attribute_settings():
    app = OldmanServer("ClassSettings", config=SettingsConfig())
    assert app.config.get("NATIVE_SHARDS") == 1
    assert app.config.FALLBACK_ERROR_FORMAT == "text"


