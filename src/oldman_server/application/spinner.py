import os
import sys
import time

from contextlib import contextmanager
from queue import Queue
from threading import Thread


class Spinner:  # noqa
    """Spinner class to show a loading spinner in the terminal.

    Used internally by the `loading` context manager.
    """

    def __init__(self, message: str) -> None:
        self.message = message
        self.queue: Queue[int] = Queue()
        self.spinner = self.cursor()
        self.thread = Thread(target=self.run)

    def start(self):
        self.queue.put(1)
        self.thread.start()
        self.hide()

    def run(self):
        while self.queue.get():
            output = f"\r{self.message} [{next(self.spinner)}]"
            sys.stdout.write(output)
            sys.stdout.flush()
            time.sleep(0.1)
            self.queue.put(1)

    def stop(self):
        self.queue.put(0)
        self.thread.join()
        self.show()

    @staticmethod
    def cursor():
        while True:
            yield from "|/-\\"

    @staticmethod
    def hide():
        if os.name == "posix":
            sys.stdout.write("\033[?25l")
            sys.stdout.flush()

    @staticmethod
    def show():
        if os.name == "posix":
            sys.stdout.write("\033[?25h")
            sys.stdout.flush()


@contextmanager
def loading(message: str = "Loading"):  # noqa
    spinner = Spinner(message)
    spinner.start()
    yield
    spinner.stop()
