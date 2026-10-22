from datetime import datetime
from typing import Any

from oldman_server import Blueprint, Request, OldmanServer
from oldman_server.response import json


def setup_health_endpoint(app: OldmanServer) -> None:
    bp = Blueprint("Health", url_prefix=app.config.HEALTH_URL_PREFIX)

    @bp.get(app.config.HEALTH_URI_TO_INFO)
    async def info(request: Request):
        return json(_make_safe(dict(request.app.m.workers)))

    app.blueprint(bp)


def _make_safe(obj: dict[str, Any]) -> dict[str, Any]:
    """Worker states as JSON values: datetimes in ISO format."""
    for key, value in obj.items():
        if isinstance(value, dict):
            obj[key] = _make_safe(value)
        elif isinstance(value, datetime):
            obj[key] = value.isoformat()
    return obj
