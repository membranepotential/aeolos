from contextlib import contextmanager
from typing import Iterator
from fabric import Connection
import boto3
from typing import Any

from .ssh import SSH


class EC2(SSH):
    def __init__(
        self,
        ami_id: str,
        instance_type: str,
        key_name: str,
        key_file: str,
        security_group: str,
        other_params: dict[str, Any] = {},
        user: str = "ubuntu",
        region: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ):
        super().__init__(
            uri="localhost",
            key_file=key_file,
            random_workdir=False,
        )

        self.ami_id = ami_id
        self.instance_type = instance_type
        self.key_name = key_name
        self.security_group = security_group
        self.other_params = other_params
        self.user = user
        self.region = region
        self._access_key = access_key
        self._secret_key = secret_key

        self._connection: Connection | None = None

    def get_ec2(self):
        return boto3.resource(
            "ec2",
            region_name=self.region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

    @contextmanager
    def setup(self) -> Iterator[str]:
        ec2 = self.get_ec2()
        instance = ec2.create_instances(
            ImageId=self.ami_id,
            MinCount=1,
            MaxCount=1,
            InstanceType=self.instance_type,
            KeyName=self.key_name,
            SecurityGroupIds=[
                self.security_group,
            ],
            **self.other_params,
        )[0]
        instance.wait_until_running()
        address = self.user + "@" + instance.public_dns_name
        yield address

        instance.terminate()

    @contextmanager
    def connect(self, address: str):
        with Connection(
            host=address,
            connect_kwargs={"key_filename": self.key_file},
        ) as self._connection:
            yield

        self._connection = None
