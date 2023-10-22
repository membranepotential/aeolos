import pytest
from moto import mock_ec2
import boto3
from uuid import uuid4

from aeolus.executor.ec2 import EC2 as EC2Executor


@pytest.fixture
def ec2_executor():
    with mock_ec2():
        client = boto3.client("ec2", region_name="us-east-1")
        response = client.create_security_group(
            Description="test", GroupName=str(uuid4())[0:6]
        )

        yield EC2Executor(
            ami_id="ami-1234",
            instance_type="g4dn.test",
            key_name="test_key",
            key_file="test_key.pem",
            security_group=response["GroupId"],
            region="us-east-1",
        )


def test_ec2_executor(ec2_executor):
    with ec2_executor.setup() as address:
        assert address
        with ec2_executor.connect(address):
            assert ec2_executor.ssh
