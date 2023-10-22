from contextlib import contextmanager
import os
import boto3
from botocore.exceptions import ClientError

from aeolos import Storage, Step


class S3(Storage):
    def __init__(
        self,
        bucket: str,
        endpoint: str | None = None,
        region: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ):
        self.bucket = bucket
        self.endpoint = endpoint
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key

    @contextmanager
    def setup(self):
        yield

    @property
    def aws_credentials(self) -> dict[str, str]:
        env = {}

        if self.endpoint is None:
            if self._region is not None:
                env["AWS_REGION"] = self._region
            elif "AWS_REGION" in os.environ:
                env["AWS_REGION"] = os.environ["AWS_REGION"]
        if self._access_key is not None:
            env["AWS_ACCESS_KEY_ID"] = self._access_key
        elif "AWS_ACCESS_KEY_ID" in os.environ:
            env["AWS_ACCESS_KEY_ID"] = os.environ["AWS_ACCESS_KEY_ID"]

        if self._secret_key is not None:
            env["AWS_SECRET_ACCESS_KEY"] = self._secret_key
        elif "AWS_SECRET_ACCESS_KEY" in os.environ:
            env["AWS_SECRET_ACCESS_KEY"] = os.environ["AWS_SECRET_ACCESS_KEY"]

        return env

    def get_command(self) -> list[str]:
        cmd = ["aws", "s3"]
        if self.endpoint is not None:
            cmd += ["--endpoint-url", self.endpoint]
        return cmd

    def pull(self, step: Step):
        s3_url = f"s3://{self.bucket}/{step.job_id}/{step.id}"
        cmd = self.get_command() + ["cp", "--recursive", s3_url, "."]
        self.command(cmd, env=self.aws_credentials, step=step)

    def push(self, step: Step):
        s3_url = f"s3://{self.bucket}/{step.job_id}/{step.id}"
        cmd = self.get_command() + ["cp", "--recursive", ".", s3_url]
        self.command(cmd, env=self.aws_credentials, step=step)

    def get_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=self._region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

    def get_meta(self, key: str) -> str:
        client = self.get_client()
        try:
            response = client.get_object(Bucket=self.bucket, Key=key)
            return response["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise KeyError(f"Metadata entry {key} does not exist")

    def set_meta(self, key: str, value: str):
        client = self.get_client()
        client.put_object(Bucket=self.bucket, Key=key, Body=value.encode("utf-8"))
