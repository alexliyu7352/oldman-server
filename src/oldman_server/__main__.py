import tracerite

try:
    tracerite.load()
except Exception as exc:
    raise RuntimeError(
        "Failed to initialize tracerite. Please verify that tracerite is "
        "correctly installed and compatible with this environment."
    ) from exc

from oldman_server.cli.app import CLI
from oldman_server.compat import OS_IS_WINDOWS, enable_windows_color_support
from oldman_server.startup.errors import maybe_handle_startup_error

if OS_IS_WINDOWS:
    enable_windows_color_support()


def main(args=None):
    try:
        cli = CLI()
        cli.attach()
        cli.run(args)
    except Exception as e:
        maybe_handle_startup_error(e)


if __name__ == "__main__":
    main()
