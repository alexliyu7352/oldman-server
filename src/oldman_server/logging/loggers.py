import logging

from oldman_server.logging.filter import VerbosityFilter


_verbosity_filter = VerbosityFilter()

logger = logging.getLogger("oldman_server.root")  # no cov
"""
oldman-server's general logger
"""
logger.addFilter(_verbosity_filter)

error_logger = logging.getLogger("oldman_server.error")  # no cov
"""
oldman-server's logger for errors
"""
error_logger.addFilter(_verbosity_filter)

access_logger = logging.getLogger("oldman_server.access")  # no cov
"""
oldman-server's access logger
"""
access_logger.addFilter(_verbosity_filter)

server_logger = logging.getLogger("oldman_server.server")  # no cov
"""
oldman-server's logger for server messages
"""
server_logger.addFilter(_verbosity_filter)

websockets_logger = logging.getLogger("oldman_server.websockets")  # no cov
"""
oldman-server's logger for WebSocket protocol messages
"""
websockets_logger.addFilter(_verbosity_filter)
websockets_logger.setLevel(logging.WARNING)  # Too noisy on debug/info
