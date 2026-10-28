from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from urllib.parse import unquote

from oldman_server.exceptions import NotFound
from oldman_server.request import Request
from oldman_server.response import file


class DirectoryHandler:
    """Serve a directory's index file.

    Args:
        uri (str): The URI to serve the files at.
        directory (Path): The directory to serve files from.
        index (str | Sequence[str] | None): The index file(s) to
            serve if the directory is requested. Defaults to None.
    """

    def __init__(
        self,
        uri: str,
        directory: Path,
        index: str | Sequence[str] | None = None,
    ) -> None:
        if isinstance(index, str):
            index = [index]
        elif index is None:
            index = []
        self.base = uri.strip("/")
        self.directory = directory
        self.index = tuple(index)

    async def handle(self, request: Request, path: str):
        """Handle the request.

        Args:
            request (Request): The incoming request object.
            path (str): The path to the file to serve.

        Raises:
            NotFound: If the directory has none of the index files, or no
                index is set.

        Returns:
            Response: The response object.
        """  # noqa: E501
        current = unquote(path).strip("/")[len(self.base) :].strip("/")  # noqa: E203
        for file_name in self.index:
            index_file = self.directory / current / file_name
            if index_file.is_file():
                return await file(index_file)

        # A directory is not a resource the static route serves.
        raise NotFound("File not found")
