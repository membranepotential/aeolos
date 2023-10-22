import pytest
from moto.server import ThreadedMotoServer
import boto3

from aeolus.storage.s3 import S3 as S3Storage
from aeolus import Step


@pytest.fixture
def s3_storage():
    endpoint = "http://localhost:5000"
    bucket_name = "test_bucket"

    server = ThreadedMotoServer()
    server.start()

    client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
    client.create_bucket(Bucket=bucket_name)

    yield S3Storage(bucket_name, endpoint=endpoint)
    server.stop()


def test_s3_storage(executor, s3_storage):
    step = Step(id="test_step", job_id="test_job", command="touch test", config={})
    with executor.launch():
        with s3_storage.in_executor(executor):
            assert not s3_storage.is_done(step)

            executor.command(step.command, step=step)
            s3_storage.store(step)
            assert s3_storage.is_done(step)

            executor.command("rm -rf test_job")
            s3_storage.pull(step)
