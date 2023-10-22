from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Iterator

from aeolus import Step
from aeolus.utils import ConfigurableObject


class NotConnectedError(Exception):
    pass


class Executor(ABC, ConfigurableObject):
    @contextmanager
    def setup(self) -> Iterator[str]:
        """
        Set up the executor.
        :return: The address of the executor.
        """
        yield ""

    @contextmanager
    def connect(self, address: str):
        """Connect to the executor."""
        yield

    @contextmanager
    def launch(self) -> Iterator[str]:
        """Connect to the executor."""
        with self.setup() as address:
            with self.connect(address):
                try:
                    yield address
                finally:
                    self.cleanup()

    def cleanup(self):
        """
        Clean up the executor.
        Commands are available.
        """
        return

    @abstractmethod
    def command(
        self,
        cmd: str | list[str],
        env: dict[str, str] | None = None,
        step: Step | None = None,
    ):
        """
        Run a shell command in the executor.
        :raises NotConnectedError: If the executor is not connected.
        """
        ...

    @abstractmethod
    def iter_logs(self, follow: bool = False) -> Iterator[str]:
        """Get all log messages"""
        ...

    def terminate(self):
        """Terminate the executor."""
        raise RuntimeError("Not supported")
