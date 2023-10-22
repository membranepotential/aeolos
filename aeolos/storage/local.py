from contextlib import contextmanager
from pathlib import Path

from .base import Storage

from aeolos import Step


class Local(Storage):
    def __init__(self, basepath: str):
        self.basepath = Path(basepath)

    @contextmanager
    def setup(self):
        self.basepath.mkdir(parents=True, exist_ok=True)
        try:
            yield
        finally:
            self.command(["cp", "__log__", self.basepath.as_posix()])

    def pull(self, step: Step):
        step_path = self.basepath / step.job_id / step.id
        self.command(["rsync", "-a", step_path.as_posix() + "/", "."], step=step)

    def push(self, step: Step):
        step_path = self.basepath / step.job_id / step.id
        step_path.mkdir(parents=True, exist_ok=True)
        self.command(["rsync", "-a", ".", step_path.as_posix()], step=step)

    def get_meta(self, key: str) -> str:
        meta_file = self.basepath / key
        if not meta_file.exists():
            raise KeyError(f"Metadata entry {key} does not exist")
        return meta_file.read_text()

    def set_meta(self, key: str, value: str):
        meta_file = self.basepath / key
        meta_file.parent.mkdir(parents=True, exist_ok=True)
        meta_file.write_text(value)
