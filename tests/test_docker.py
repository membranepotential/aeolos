from aeolus.repository.docker import Docker
from aeolus import Step


def test_docker(executor):
    repository = Docker()
    step = Step(
        id="test_step",
        job_id="test_job",
        command="hello-world",
        config={},
    )

    with executor.launch():
        with repository.in_executor(executor):
            repository.run(step)
