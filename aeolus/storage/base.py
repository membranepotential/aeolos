from abc import ABC, abstractmethod

from aeolus import Step
from aeolus.utils import ConfigurableObject
from aeolus.executor.executing import Executing


class Storage(ABC, ConfigurableObject, Executing):
    @abstractmethod
    def pull(self, step: Step):
        """Pull the result of a step from the storage to the executor"""
        ...

    @abstractmethod
    def push(self, step: Step):
        """Push the result of a step from the executor to the storage"""
        ...

    @abstractmethod
    def get_meta(self, key: str):
        """Get a metadata entry"""
        ...

    @abstractmethod
    def set_meta(self, key: str, value: str):
        """Set a metadata entry"""
        ...

    def is_done(self, step: Step) -> bool:
        """Check if a step is done"""
        try:
            sha256 = self.get_meta(f"{step.job_id}/{step.id}/__done__")
            return sha256 == step.sha256()
        except KeyError:
            return False

    def mark_done(self, step: Step) -> bool:
        """Mark a step as done"""
        self.set_meta(f"{step.job_id}/{step.id}/__done__", step.sha256())

    def store(self, step: Step):
        """Put the result of a step in the storage"""
        self.push(step)
        self.mark_done(step)
