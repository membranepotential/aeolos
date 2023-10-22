from contextlib import contextmanager
from dataclasses import dataclass
import json
from typing import Any

from aeolus import Stage, Task, Job, Executor, Storage, Repository
from aeolus.utils import ConfigurableObject
from aeolus.executor.executing import Executing


@dataclass
class AeolusContext:
    config: dict[str, Any]
    executor: Executor
    storage: Storage
    repository: Repository

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "AeolusContext":
        return cls(
            config=config,
            executor=ConfigurableObject.load_dict(config["executor"]),
            storage=ConfigurableObject.load_dict(config["storage"]),
            repository=ConfigurableObject.load_dict(config["repository"]),
        )

    @property
    def task(self) -> Task:
        try:
            stages = [Stage(**stage) for stage in self.config["stages"]]
            return Task(stages)
        except KeyError:
            raise ValueError("Task configuration with key 'stages' missing")
        except TypeError:
            raise ValueError("Invalid task configuration")

    @property
    def job(self) -> Job:
        try:
            config = self.config["config"]
        except KeyError:
            raise ValueError("Job configuration with key 'config' missing")
        try:
            job_id = config["__id__"]
        except KeyError:
            raise ValueError("Job ID with key '__id__' missing in job configuration")

        return Job(stages=self.task.stages, id=job_id, config=config)

    @contextmanager
    def bind_executor(self, obj: Executing, executor: Executor, active: bool):
        if active:
            with obj.in_executor(executor):
                yield
        else:
            yield

    @contextmanager
    def connect(self, address: str, storage: bool = True, repository: bool = True):
        with self.executor.connect(address):
            with self.bind_executor(self.storage, self.executor, storage):
                with self.bind_executor(self.repository, self.executor, repository):
                    yield

    @contextmanager
    def launch(self):
        with self.executor.setup() as address:
            with self.connect(address):
                try:
                    yield address
                finally:
                    self.executor.cleanup()

    def as_dict(self) -> dict[str, Any]:
        return {
            "executor": self.executor.as_dict(),
            "storage": self.storage.as_dict(),
            "repository": self.repository.as_dict(),
            "stages": self.config["stages"],
            "config": self.config["config"],
        }

    def as_json(self, indent: int | str | None = None) -> str:
        return json.dumps(self.as_dict(), indent=indent, default=str)
