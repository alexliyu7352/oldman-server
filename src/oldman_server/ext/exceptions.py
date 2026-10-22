from oldman_server.exceptions import OldmanServerException


class ValidationError(OldmanServerException):
    status_code = 400


class InitError(OldmanServerException): ...


class ExtensionNotFound(OldmanServerException): ...
