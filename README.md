# Aeolus

CLI to run a _task_ with an _executor_.
A _repository_ provides executables, e.g. docker container.
Results are stored on a _Storage_.

## Usage

Configure using JSON files (`--config/-c`) or JSON strings (`--json/-j`).
The config must include the top level keys `executor, repository, storage, stages, config`.

## Examples

- [local execution](./example_config.json)
