from abc import ABC, abstractmethod

from aeolus import Step
from aeolus.utils import ConfigurableObject
from aeolus.executor.executing import Executing


class Repository(ABC, ConfigurableObject, Executing):
    @abstractmethod
    def run(self, step: Step):
        """Run a step"""
        ...
