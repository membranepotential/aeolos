import docker
import pytest
import time

from aeolus.executor.ec2 import SSH as SSHExecutor


@pytest.fixture
def ssh_server():
    client = docker.DockerClient()
    client.images.pull("linuxserver/openssh-server")
    container = client.containers.run(
        "linuxserver/openssh-server",
        detach=True,
        remove=True,
        environment={
            "USER_NAME": "user",
            "USER_PASSWORD": "password",
            "PASSWORD_ACCESS": "true",
        },
        ports={"2222/tcp": None},
    )

    try:
        time.sleep(1.0)
        ip = client.containers.get(container.id).attrs["NetworkSettings"]["IPAddress"]
        yield "user@" + ip + ":2222"
    finally:
        container.stop()


@pytest.fixture
def ssh_executor(ssh_server):
    yield SSHExecutor(uri=ssh_server, password="password")


def test_ssh_executor(ssh_executor):
    with ssh_executor.launch():
        ssh_executor.command('echo "hello\nworld"')
        logs = list(ssh_executor.iter_logs())
        assert logs == ["hello", "world"]
