from abc import ABC, abstractmethod

from html5tagger import HTML, Builder, Document

from oldman_server import __version__ as VERSION
from oldman_server.pages.css import CSS


class BasePage(ABC, metaclass=CSS):  # no cov
    """Base page for the framework's HTML pages."""

    TITLE = "oldman-server"
    HEADING = None
    CSS: str
    doc: Builder

    def __init__(self, debug: bool = True) -> None:
        self.debug = debug

    @property
    def style(self) -> str:
        """Returns the CSS for the page.

        Returns:
            str: The CSS for the page.
        """
        return self.CSS

    def render(self) -> str:
        """Renders the page.

        Returns:
            str: The rendered page.
        """
        self.doc = Document(self.TITLE, lang="en", id="oldman-server")
        self._head()
        self._body()
        self._foot()
        return str(self.doc)

    def _head(self) -> None:
        self.doc.style(HTML(self.style))
        with self.doc.header:
            self.doc.div(self.HEADING or self.TITLE)

    def _foot(self) -> None:
        with self.doc.footer:
            self.doc.div("powered by oldman-server")
            if self.debug:
                self.doc.div(f"Version {VERSION}")
                self.doc.div("DEBUG mode")

    @abstractmethod
    def _body(self) -> None: ...
