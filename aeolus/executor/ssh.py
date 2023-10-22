from contextlib import contextmanager
from typing import Iterator
from fabric import Connection
from secrets import token_hex

from aeolus import Executor, Step


class SSH(Executor):
    def __init__(
        self,
        uri: str = "localhost",
        password: str | None = None,
        key_file: str | None = None,
        random_workdir: bool = False,
    ):
        self.uri = uri
        self.password = password
        self.key_file = key_file
        self.random_workdir = random_workdir

        self._connection: Connection | None = None
        self._workdir: str | None = None

    @property
    def ssh(self):
        if self._connection is None:
            raise RuntimeError("Not connected")
        return self._connection

    @property
    def workdir(self) -> str:
        if self._workdir is None:
            raise RuntimeError("Not set up")
        return self._workdir

    @property
    def logfile(self) -> str:
        return self.workdir + "/__log__"

    @contextmanager
    def setup(self) -> Iterator[str]:
        address = self.uri
        if self.random_workdir:
            workdir = token_hex(4)
            address += "/" + workdir
        yield address

    @contextmanager
    def connect(self, address: str):
        connect_kwargs = {}
        if self.password is not None:
            connect_kwargs["password"] = self.password
        if self.key_file is not None:
            connect_kwargs["key_filename"] = self.key_file

        address_parts = address.split("/", 1)
        uri = address_parts[0]
        if len(address_parts) == 2:
            workdir = address_parts[1]
        else:
            workdir = "."

        with Connection(host=uri, connect_kwargs=connect_kwargs) as self._connection:
            result = self.ssh.run(
                f"mkdir -p {workdir} && realpath {workdir}",
                in_stream=False,
            )
            self._workdir = result.stdout.strip()

            yield
            self._workdir = None

        self._connection = None

    def command(
        self,
        cmd: str | list[str],
        env: dict[str, str] | None = None,
        step: Step | None = None,
    ):
        cwd = "" if step is None else f"{step.job_id}/{step.id}"
        workdir = f"{self.workdir}/{cwd}"

        if isinstance(cmd, list):
            cmd = " ".join(cmd)

        cmd = f"""
        mkdir -p {workdir} &&
        cd {workdir} &&
        {cmd} >> {self.logfile}
        """
        self.ssh.run(cmd.strip(), env=env, in_stream=False, out_stream=False)

    def iter_logs(self, follow: bool = False) -> Iterator[str]:
        cmd = f"cat {self.logfile}"

        proc = self.ssh.run(cmd, in_stream=False)
        for line in proc.stdout.split("\n"):
            line = line.strip()
            if line:
                yield line

    def terminate(self):
        if self._connection is None:
            raise RuntimeError("Not connected")
        self._connection.close()
