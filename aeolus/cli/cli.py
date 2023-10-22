from typing import Any, List, Optional
from pathlib import Path
import json

import typer
from typing_extensions import Annotated

from ..context import AeolusContext


class DuplicateConfigError(ValueError):
    pass


ctx: Optional[AeolusContext] = None


def add_to_config(config: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    # check non duplicate top level keys
    dupes = config.keys() & extra.keys()
    if dupes:
        raise DuplicateConfigError(f"Duplicate keys {', '.join(dupes)}")
    return config | extra


def read_configs(
    configs: list[str],
    jsons: list[str],
) -> dict[str, Any]:
    config = {}
    for path in configs:
        with open(path) as f:
            loaded = json.load(f)
        try:
            config = add_to_config(config, loaded)
        except DuplicateConfigError as e:
            raise DuplicateConfigError(e.args[0] + f" in {path}")

    for config_str in jsons:
        loaded = json.loads(config_str)
        config = add_to_config(config, loaded)

    return config


app = typer.Typer()


@app.callback()
def cli(
    configs: Annotated[List[Path], typer.Option("--config", "-c")] = [],
    jsons: Annotated[List[str], typer.Option("--json", "-j")] = [],
):
    config = read_configs(configs, jsons)

    global ctx
    ctx = AeolusContext.from_config(config)


@app.command()
def launch():
    job = ctx.job

    with ctx.launch() as address:
        print(f"[executor] {address}")

        ctx.storage.set_meta(job.id + "/__address__", address)
        print(f"[job {job.id}] starting")

        for step in ctx.job:
            ctx.storage.set_meta(job.id + "/__step__", step.id)

            if ctx.storage.is_done(step):
                print(f"[step {step.id}] in storage")
                ctx.storage.pull(step)
            else:
                print(f"[step {step.id}] start")
                ctx.repository.run(step)
                ctx.storage.store(step)

            print(f"[step {step.id}] done")

        ctx.storage.set_meta(job.id + "/__step__", "")
        ctx.storage.set_meta(job.id + "/__logs__", "\n".join(ctx.executor.iter_logs()))
        print(f"[job {job.id}] done")


@app.command()
def status():
    steps = {}
    for step in ctx.job:
        if ctx.storage.is_done(step):
            steps[step.id] = "done"
        else:
            steps[step.id] = "pending"

    print(json.dumps(steps, indent=2))


@app.command()
def logs():
    address = ctx.storage.get_meta(ctx.job.id + "/__address__")
    with ctx.connect(address, storage=False, repository=False):
        for msg in ctx.executor.iter_logs():
            print(msg)


@app.command()
def terminate():
    address = ctx.storage.get_meta(ctx.job.id + "/__address__")
    with ctx.connect(address, storage=False, repository=False):
        print("[executor] connected")
        ctx.executor.terminate()
