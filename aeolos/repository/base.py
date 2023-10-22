from abc import ABC, abstractmethod

from aeolos import Step
from aeolos.utils import ConfigurableObject
from aeolos.executor.executing import Executing


class Repository(ABC, ConfigurableObject, Executing):
    @abstractmethod
    def run(self, step: Step):
        """Run a step"""
        ...
