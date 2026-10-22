from __future__ import annotations

from argparse import ArgumentParser, _ArgumentGroup

from sanic_routing import __version__ as __routing_version__

from oldman_server import __version__


class Group:
    name: str | None
    container: ArgumentParser | _ArgumentGroup
    _registry: list[type[Group]] = []

    def __init_subclass__(cls) -> None:
        Group._registry.append(cls)

    def __init__(self, parser: ArgumentParser, title: str | None):
        self.parser = parser

        if title:
            self.container = self.parser.add_argument_group(title=f"  {title}")
        else:
            self.container = self.parser

    @classmethod
    def create(cls, parser: ArgumentParser):
        instance = cls(parser, cls.name)
        return instance

    def add_bool_arguments(
        self,
        *args,
        nullable=False,
        help: str,
        negative_help: str | None = None,
        **kwargs,
    ):
        group = self.container.add_mutually_exclusive_group()

        pos_help = help[0].upper() + help[1:]
        neg_help = (
            negative_help if negative_help else f"Disable {help.lower()}"
        )

        group.add_argument(
            *args,
            action="store_true",
            help=pos_help,
            **kwargs,
        )

        group.add_argument(
            "--no-" + args[0][2:],
            *args[1:],
            action="store_false",
            help=neg_help[0].upper() + neg_help[1:],
            **kwargs,
        )

        if nullable:
            group.set_defaults(**{args[0][2:].replace("-", "_"): None})

    def prepare(self, args) -> None: ...

    def attach(self, short: bool = False) -> None: ...


class GeneralGroup(Group):
    name = None

    def attach(self, short: bool = False):
        if short:
            return

        self.container.add_argument(
            "--version",
            action="version",
            version=f"oldman-server {__version__}; Routing {__routing_version__}",
        )

        self.container.add_argument(
            "target",
            help=(
                "Path to your app instance.\n"
                "\tExample: path.to.server:app\n"
                "If running a Simple Server, path to directory to serve.\n"
                "\tExample: ./\n"
                "Additionally, this can be a path to a factory function\n"
                "that returns an app instance.\n"
                "\tExample: path.to.server:create_app\n"
            ),
        )

        choices = ["serve", "exec"]
        help_text = (
            "Action to perform.\n"
            "\tserve: Run the app [default]\n"
            "\texec: Execute a command in the app context\n"
        )
        self.container.add_argument(
            "action",
            nargs="?",
            default="serve",
            choices=choices,
            help=help_text,
        )


class ApplicationGroup(Group):
    name = "Application"

    def attach(self, short: bool = False):
        if short:
            return

        group = self.container.add_mutually_exclusive_group()
        group.add_argument(
            "--factory",
            action="store_true",
            help=(
                "Treat app as an application factory, "
                "i.e. a () -> <OldmanServer app> callable"
            ),
        )
        group.add_argument(
            "-s",
            "--simple",
            dest="simple",
            action="store_true",
            help=(
                "Run as a simple server, and serve the contents of "
                "a directory\n(module arg should be a path)"
            ),
        )


class SocketGroup(Group):
    name = "Socket binding"

    def attach(self, short: bool = False):
        self.container.add_argument(
            "-H",
            "--host",
            dest="host",
            type=str,
            help="Host address [default 127.0.0.1]",
        )
        self.container.add_argument(
            "-p",
            "--port",
            dest="port",
            type=int,
            help="Port to serve on [default 8000]",
        )
        if not short:
            self.container.add_argument(
                "-u",
                "--unix",
                dest="unix",
                type=str,
                default="",
                help="location of UNIX socket",
            )


class DevelopmentGroup(Group):
    name = "Development"

    def attach(self, short: bool = False):
        if not short:
            self.container.add_argument(
                "--debug",
                dest="debug",
                action="store_true",
                help="Run the server in debug mode",
            )
            self.container.add_argument(
                "-r",
                "--reload",
                "--auto-reload",
                dest="auto_reload",
                action="store_true",
                help="Auto-reload on source changes",
            )
            self.container.add_argument(
                "-R",
                "--reload-dir",
                dest="path",
                action="append",
                help="Additional directories to watch for changes",
            )
        self.container.add_argument(
            "-d",
            "--dev",
            dest="dev",
            action="store_true",
            help="Run in development mode (debug + auto-reload)",
        )


class WorkerGroup(Group):
    name = "Worker"

    def attach(self, short: bool = False):
        if short:
            return

        group = self.container.add_mutually_exclusive_group()
        group.add_argument(
            "-w",
            "--workers",
            dest="workers",
            type=int,
            default=1,
            help="Number of worker processes [default 1]",
        )
        group.add_argument(
            "--fast",
            dest="fast",
            action="store_true",
            help="Set the number of workers to max allowed",
        )
        group.add_argument(
            "--single-process",
            dest="single",
            action="store_true",
            help="Do not use multiprocessing, run server in a single process",
        )
        self.add_bool_arguments(
            "--access-logs",
            dest="access_log",
            help="display access logs",
            default=None,
        )


class OutputGroup(Group):
    name = "Output"

    def attach(self, short: bool = False):
        if short:
            return

        self.add_bool_arguments(
            "--motd",
            dest="motd",
            default=True,
            help="Show the startup display",
            negative_help="Disable the startup display",
        )
        self.container.add_argument(
            "-v",
            "--verbosity",
            action="count",
            help="Control logging noise, eg. -vv or --verbosity=2 [default 0]",
        )
        self.add_bool_arguments(
            "--noisy-exceptions",
            dest="noisy_exceptions",
            help="Output stack traces for all exceptions",
            default=None,
        )
