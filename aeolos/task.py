from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Iterator


@dataclass
class Stage:
    """
    A stage is a procedure that is
        - executed on an executor
        - pulled from a repository
        - acting on data stored in a storage
    """

    id: str
    command: str

    def __post_init__(self):
        if not self.id.isidentifier():
            raise ValueError(
                f"Stage id must resemble valid pyhon identifier: {self.id}"
            )


@dataclass
class Step(Stage):
    """A configured stage"""

    job_id: str
    config: dict[str, Any]

    @classmethod
    def from_stage(cls, stage: Stage, **kwargs) -> "Step":
        return cls(**vars(stage), **kwargs)

    def sha256(self) -> str:
        data = "\n".join(k + ":" + str(self.config[k]) for k in sorted(self.config))
        data = self.id + "\n" + self.command + "\n" + data
        return sha256(data.encode()).hexdigest()

    def format_command(self):
        return self.command.format(**self.config)


@dataclass
class Task:
    """A sequence of stages"""

    stages: list[Stage]

    def __post_init__(self):
        stage_ids = {stage.id for stage in self.stages}
        if len(stage_ids) != len(self.stages):
            raise ValueError("Duplicate stage id")

    def __iter__(self) -> Iterator[Stage]:
        return iter(self.stages)


@dataclass
class Job(Task):
    """A configured task"""

    id: str
    config: dict[str, dict[str, Any]]

    def from_task(self, task: Task, **kwargs) -> "Job":
        return Job(**vars(task), **kwargs)

    def __iter__(self) -> Iterator[Step]:
        for stage in self.stages:
            config = self.config.get(stage.id, {})
            yield Step.from_stage(stage, job_id=self.id, config=config)

    def __getitem__(self, k: str | int) -> Step:
        if isinstance(k, int):
            stage = self.stages[k]
        elif isinstance(k, str):
            stage = next(stage for stage in self.stages if stage.id == k)
        else:
            raise TypeError(f"Invalid index type: {k}: {type(k)}")

        config = self.config.get(stage.id, {})
        return Step.from_stage(stage, job_id=self.id, config=config)
