from tempfile import TemporaryDirectory
import pytest

from aeolos.executor.local import Local as LocalExecutor
from aeolos.repository.local import Local as LocalRepository
from aeolos.storage.local import Local as LocalStorage
from aeolos.context import AeolosContext


@pytest.fixture
def executor():
    return LocalExecutor()


@pytest.fixture
def repository():
    return LocalRepository()


@pytest.fixture
def storage() -> LocalStorage:
    with TemporaryDirectory() as tmpdir:
        return LocalStorage(tmpdir)


@pytest.fixture
def config():
    return {
        "stages": [
            {"id": "test_step", "command": "touch {file}"},
        ],
        "config": {
            "__id__": "test_job",
            "test_step": {"file": "test"},
        },
    }


@pytest.fixture
def long_running_config():
    return {
        "stages": [
            {"id": "touch_1", "command": "touch test"},
            {"id": "sleep", "command": "sleep {seconds}"},
            {"id": "touch_2", "command": "touch test"},
        ],
        "config": {
            "__id__": "test_job",
            "sleep": {"seconds": 2},
        },
    }


@pytest.fixture
def context(executor, repository, storage, config):
    return AeolosContext(
        config=config,
        executor=executor,
        repository=repository,
        storage=storage,
    )


@pytest.fixture
def long_running_context(executor, repository, storage, long_running_config):
    return AeolosContext(
        config=long_running_config,
        executor=executor,
        repository=repository,
        storage=storage,
    )
