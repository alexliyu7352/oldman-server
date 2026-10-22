from __future__ import annotations

import os
import shutil
import sys

from argparse import Namespace
from functools import partial
from textwrap import indent

from oldman_server.app import OldmanServer
from oldman_server.application.logo import get_logo
from oldman_server.cli.arguments import Group
from oldman_server.cli.base import CLIArgumentParser, CLIHelpFormatter
from oldman_server.cli.executor import Executor, make_executor_parser
from oldman_server.log import error_logger
from oldman_server.worker.loader import AppLoader


class CLI:
    DESCRIPTION_SHORT = indent(
        f"""
{get_logo()}

Usage:
    oldman-server <target> [options]   Run an application
    oldman-server <target> exec <cmd>  Execute a command in app context
    oldman-server help [--full]        Show help (--full for all options)

Examples:
    oldman-server path.to.server:app    Run app
    oldman-server path.to.server --dev  Run in development mode
    oldman-server ./static --simple     Serve static files
""",
        prefix=" ",
    )

    DESCRIPTION_SHORT_FOOTER = """
(additional options available)

For complete options and documentation:
    oldman-server help --full
"""

    DESCRIPTION_FULL = indent(
        f"""
{get_logo()}

To start running an application, provide a path to the module, where
app is an OldmanServer() instance in the global scope:

    $ oldman-server path.to.server:app

If the OldmanServer instance variable is called 'app', you can leave off the last
part, and only provide a path to the module where the instance is:

    $ oldman-server path.to.server

Or, a path to a callable that returns an OldmanServer() instance:

    $ oldman-server path.to.factory:create_app

Or, a path to a directory to run as a simple HTTP server:

    $ oldman-server ./path/to/static

Additional commands:

    $ oldman-server path.to.app exec ...  Run app commands
""",
        prefix=" ",
    )

    def __init__(self) -> None:
        width = shutil.get_terminal_size().columns
        self.parser = CLIArgumentParser(
            prog="oldman-server",
            description=self.DESCRIPTION_SHORT,
            formatter_class=lambda prog: CLIHelpFormatter(
                prog,
                max_help_position=36 if width > 96 else 24,
                indent_increment=4,
                width=None,
            ),
        )
        self.parser._positionals.title = "Required\n========\n  Positional"
        self.parser._optionals.title = "Optional\n========\n  General"
        self.main_process = (
            os.environ.get("OLDMAN_SERVER_RELOADER_PROCESS", "") != "true"
        )
        self.args: Namespace = Namespace()
        self.groups: list[Group] = []
        self.run_mode = "serve"

    def attach(self):
        if len(sys.argv) > 1 and sys.argv[1] == "help":
            self.run_mode = "help"
            return

        if len(sys.argv) == 2 and sys.argv[1] in ("-h", "--help"):
            self.run_mode = "help"
            return

        for group in Group._registry:
            instance = group.create(self.parser)
            instance.attach()
            self.groups.append(instance)

        if len(sys.argv) > 2 and sys.argv[2] == "exec":
            self.run_mode = "exec"
            self.parser.description = get_logo()
            make_executor_parser(self.parser)

    def run(self, parse_args=None):
        if self.run_mode == "help":
            self._help()
            return

        legacy_version = False
        if not parse_args:
            # This is to provide backwards compat -v to display version
            legacy_version = len(sys.argv) == 2 and sys.argv[-1] == "-v"
            parse_args = ["--version"] if legacy_version else None
        elif parse_args == ["-v"]:
            parse_args = ["--version"]

        if not legacy_version:
            if self.run_mode == "exec":
                parse_args = [
                    a
                    for a in (parse_args or sys.argv[1:])
                    if a not in "-h --help".split()
                ]
            parsed, unknown = self.parser.parse_known_args(args=parse_args)
            if unknown and parsed.factory:
                for arg in unknown:
                    if arg.startswith("--"):
                        self.parser.add_argument(arg.split("=")[0])

        if self.run_mode == "exec":
            self.args, _ = self.parser.parse_known_args(args=parse_args)
        else:
            self.args = self.parser.parse_args(args=parse_args)
        app_loader = AppLoader(
            self.args.target, self.args.factory, self.args.simple, self.args
        )

        try:
            app = self._get_app(app_loader)
            kwargs = self._build_run_kwargs()
            if (
                not kwargs["single_process"]
                and app.native_single_process(
                    kwargs["workers"], kwargs["fast"], kwargs.get("dev", False),
                    kwargs.get("auto_reload"),
                )
            ):
                # The native host serves from this process (see Sanic.run).
                self.args.single = kwargs["single_process"] = True
                app.state.native_main_process = True
        except ValueError as e:
            error_logger.exception(f"Failed to run app: {e}")
        else:
            if self.run_mode == "exec":
                self._executor(app, kwargs)
                return
            elif self.run_mode != "serve":
                raise ValueError(f"Unknown run mode: {self.run_mode}")

            app.prepare(**kwargs)

            if self.args.single:
                serve = OldmanServer.serve_single
            else:
                serve = partial(OldmanServer.serve, app_loader=app_loader)
            serve(app)

    def _help(self):
        full = "--full" in sys.argv
        if full:
            self.parser.description = self.DESCRIPTION_FULL
        for group in Group._registry:
            instance = group.create(self.parser)
            instance.attach(short=not full)
            self.groups.append(instance)
        self.parser.print_help()
        if not full:
            print(self.DESCRIPTION_SHORT_FOOTER)

    def _executor(self, app: OldmanServer, kwargs: dict):
        args = sys.argv[3:]
        Executor(app, kwargs).run(self.args.command, args)

    def _get_app(self, app_loader: AppLoader):
        try:
            app = app_loader.load()
        except ImportError as e:
            if app_loader.module_name.startswith(e.name):  # type: ignore
                error_logger.error(
                    f"No module named {e.name} found.\n"
                    "  Example File: project/sanic_server.py -> app\n"
                    "  Example Module: project.sanic_server.app"
                )
                error_logger.error(
                    "\nThe error below might have caused the above one:\n"
                    f"{e.msg}"
                )
                sys.exit(1)
            else:
                raise e
        return app

    def _build_run_kwargs(self):
        for group in self.groups:
            group.prepare(self.args)
        kwargs = {
            "access_log": self.args.access_log,
            "debug": self.args.debug,
            "fast": self.args.fast,
            "host": self.args.host,
            "motd": self.args.motd,
            "noisy_exceptions": self.args.noisy_exceptions,
            "port": self.args.port,
            "unix": self.args.unix,
            "verbosity": self.args.verbosity or 0,
            "workers": self.args.workers,
            "single_process": self.args.single,
        }

        for maybe_arg in ("auto_reload", "dev"):
            if getattr(self.args, maybe_arg, False):
                kwargs[maybe_arg] = True

        if self.args.path:
            kwargs["auto_reload"] = True
            kwargs["reload_dir"] = self.args.path

        return kwargs
