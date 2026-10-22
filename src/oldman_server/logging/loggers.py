import logging

from oldman_server.logging.filter import VerbosityFilter


_verbosity_filter = VerbosityFilter()

logger = logging.getLogger("oldman_server.root")  # no cov
"""
General Sanic logger
"""
logger.addFilter(_verbosity_filter)

error_logger = logging.getLogger("oldman_server.error")  # no cov
"""
Logger used by Sanic for error logging
"""
error_logger.addFilter(_verbosity_filter)

access_logger = logging.getLogger("oldman_server.access")  # no cov
"""
Logger used by Sanic for access logging
"""
access_logger.addFilter(_verbosity_filter)

server_logger = logging.getLogger("oldman_server.server")  # no cov
"""
Logger used by Sanic for server related messages
"""
server_logger.addFilter(_verbosity_filter)

websockets_logger = logging.getLogger("oldman_server.websockets")  # no cov
"""
Logger used by Sanic for websockets module and protocol related messages
"""
websockets_logger.addFilter(_verbosity_filter)
websockets_logger.setLevel(logging.WARNING)  # Too noisy on debug/info
