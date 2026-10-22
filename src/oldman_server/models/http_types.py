from __future__ import annotations

from base64 import b64decode
from dataclasses import dataclass, field


@dataclass()
class Credentials:
    auth_type: str | None
    token: str | None
    _username: str | None = field(default=None)
    _password: str | None = field(default=None)

    def __post_init__(self):
        if self._auth_is_basic:
            # RFC 7617: base64 of "user-id:password"; the password may
            # contain colons. Malformed credentials raise ValueError.
            decoded = b64decode(self.token.encode("utf-8"), validate=True).decode()
            username, colon, password = decoded.partition(":")
            if not colon:
                raise ValueError("Basic credentials without a colon")
            self._username, self._password = username, password

    @property
    def username(self):
        if not self._auth_is_basic:
            raise AttributeError("Username is available for Basic Auth only")
        return self._username

    @property
    def password(self):
        if not self._auth_is_basic:
            raise AttributeError("Password is available for Basic Auth only")
        return self._password

    @property
    def _auth_is_basic(self) -> bool:
        return self.auth_type == "Basic"
