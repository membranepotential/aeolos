from contextlib import contextmanager
from pathlib import Path
import shlex
from subprocess import run
from tempfile import TemporaryDirectory
from typing import Iterator, TextIO
from psutil import Process
import time

from aeolus import Executor, Step


class Local(Executor):
    def __init__(self):
        self._workdir: Path | None = None
        self._log_file: TextIO | None = None

    @property
    def workdir(self) -> Path:
        if self._workdir is None:
            raise RuntimeError("Not connected")
        return self._workdir

    @property
    def log_file_path(self) -> Path:
        return self.workdir / "__log__"

    @property
    def process(self) -> Process:
        """Returns the executing process"""
        pid_file = Path(self.workdir) / "__pid__"
        return Process(int(pid_file.read_text()))

    @contextmanager
    def setup(self) -> Iterator[str]:
        with TemporaryDirectory(prefix="aeolus_") as workdir:
            self._process = Process()

            pid_file = Path(workdir) / "__pid__"
            pid_file.write_text(str(self._process.pid))

            yield workdir

    @contextmanager
    def connect(self, address: str):
        self._workdir = Path(address)
        with self.log_file_path.open("a") as self._log_file:
            yield

        self._workdir = None
        self._log_file = None

    def command(
        self,
        cmd: str | list[str],
        env: dict[str, str] | None = None,
        step: Step | None = None,
    ):
        workdir = self.workdir
        if step is not None:
            workdir = workdir / step.job_id / step.id
            workdir.mkdir(parents=True, exist_ok=True)

        if isinstance(cmd, str):
            cmd = shlex.split(cmd)

        run(
            cmd,
            cwd=workdir,
            env=env,
            stdout=self._log_file,
            stderr=self._log_file,
            check=True,
        )

    def iter_logs(self, follow: bool = False) -> Iterator[str]:
        with self.log_file_path.open() as log_file:
            yield from log_file.readlines()

            if follow:
                process = self.process
                while True:
                    if not process.is_running():
                        break

                    line = log_file.readline()
                    if line:
                        yield line
                    else:
                        time.sleep(0.1)

                yield from log_file.readlines()

    def terminate(self):
        self.process.terminate()
