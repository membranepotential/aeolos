from contextlib import contextmanager

from aeolos import Executor, Step


class Executing:
    """An object that can be bound to an executor."""

    _executor: Executor | None = None

    @contextmanager
    def in_executor(self, executor: Executor):
        """Run in an executor."""
        self._executor = executor
        try:
            with self.setup():
                yield
        finally:
            self._executor = None

    @contextmanager
    def setup(self):
        """Set up the bindable."""
        yield

    @property
    def executor(self) -> Executor:
        """Get the executor."""
        if self._executor is None:
            raise RuntimeError("Executor required")
        return self._executor

    def command(
        self,
        cmd: str | list[str],
        env: dict[str, str] | None = None,
        step: Step | None = None,
    ):
        """Run a shell command in the executor."""
        self.executor.command(cmd, env=env, step=step)
