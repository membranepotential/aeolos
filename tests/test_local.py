from tempfile import TemporaryDirectory
import pytest
import os
import json
from subprocess import Popen, run
import time

from aeolus.executor.local import Local as LocalExecutor
from aeolus.repository.local import Local as LocalRepository
from aeolus.storage.local import Local as LocalStorage
from aeolus.context import AeolusContext

AEOLUS_CMD = ["poetry", "run", "aeolus"]


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
    return AeolusContext(
        config=config,
        executor=executor,
        repository=repository,
        storage=storage,
    )


@pytest.fixture
def long_running_context(executor, repository, storage, long_running_config):
    return AeolusContext(
        config=long_running_config,
        executor=executor,
        repository=repository,
        storage=storage,
    )


def test_storage(context: AeolusContext):
    executor = context.executor
    storage = context.storage
    step = context.job[0]

    with context.launch():
        assert os.listdir(storage.basepath) == []
        assert not storage.is_done(step)

        executor.command(step.format_command(), step=step)
        assert os.listdir(executor.workdir / "test_job" / "test_step") == ["test"]

        storage.store(step)
        assert set(os.listdir(storage.basepath / "test_job" / "test_step")) == {
            "test",
            "__done__",
        }
        assert storage.is_done(step)

    with context.launch():
        assert not (executor.workdir / "test_job").exists()
        storage.pull(step)
        assert set(os.listdir(executor.workdir / "test_job" / "test_step")) == {
            "test",
            "__done__",
        }


def test_launch(context):
    config = context.as_json()
    print(config)
    proc = run(
        AEOLUS_CMD + ["-j", config, "launch"],
        capture_output=True,
    )
    print(proc.stdout.decode())
    print(proc.stderr.decode())
    assert proc.returncode == 0


def test_status(long_running_context):
    config = long_running_context.as_json()
    Popen(AEOLUS_CMD + ["-j", config, "launch"])

    time.sleep(0.1)
    proc = run(AEOLUS_CMD + ["-j", config, "status"], capture_output=True, check=True)
    status = json.loads(proc.stdout)
    assert status == {"touch_1": "done", "sleep": "pending", "touch_2": "pending"}


def test_terminate(long_running_context):
    config = long_running_context.as_json()
    proc = Popen(AEOLUS_CMD + ["-j", config, "launch"])

    time.sleep(0.1)
    assert proc.poll() is None

    run(AEOLUS_CMD + ["-j", config, "terminate"], capture_output=True, check=True)
    assert proc.poll() is not None
