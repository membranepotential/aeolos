from contextlib import contextmanager
import shlex

from aeolus import Step, Repository


class Docker(Repository):
    def __init__(self, url: str = ""):
        self.url = url

    @contextmanager
    def setup(self):
        yield

    def pull(self, step: Step):
        image = step.command
        if self.url:
            image = self.url + "/" + image
        if "tag" in step.config:
            image += ":" + step.config["tag"]

        self.command(["docker", "pull", image], step=step)
        return image

    def run(self, step: Step):
        self.command("echo", step=step)

        args = []
        if "workspace" in step.config:
            args += ["-v", f"{step.job_id}/{step.id}:{step.config['workspace']}"]

        for key, val in step.config.get("env", {}).items():
            if val:
                args += ["-e", f"{key}={val}"]
            else:
                args += ["-e", key]

        if "docker_args" in step.config:
            args += step.config["docker_args"]

        image = self.pull(step)
        args += [image]

        if "command" in step.config:
            args += shlex.split(step.config["command"])

        self.command(["docker", "run", "--rm", *args], step=step)
