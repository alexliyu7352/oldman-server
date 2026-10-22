from oldman_server import Request
from oldman_server.exceptions import OldmanServerException


def extract_request(*args) -> Request:
    request: Request
    if args and isinstance(args[0], Request):
        request = args[0]
    elif len(args) > 1:
        request = args[1]
    else:
        raise OldmanServerException("Request could not be found")
    return request
