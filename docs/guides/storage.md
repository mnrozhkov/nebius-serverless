# Storage and files

## Mount a bucket or a volume

`volumes` takes mount strings and passes them through: `source:container_path[:mode]`, or `s3://bucket:/container_path[:mode]` for an Object Storage bucket. The mode is `ro` or `rw` (default `rw`). The client does not check that the objects exist.

```python
spec = ns.JobSpec(
    ["python", "train.py"],
    image="registry.example.com/team/trainer:1.4",
    gpu="h100",
    timeout="24h",
    volumes=[
        "s3://my-data:/workspace/data:ro",
        "s3://my-results:/workspace/results:rw",
    ],
)
```

An `s3://` string with a fourth `:profile` part raises `UnsupportedFeature`. A stopped endpoint is not billed for compute or its disk, but mounted volumes still bill.

!!! tip "Write to local disk, then copy to the bucket"
    Some tools append to files as they run, which the S3 mount does not support. Axolotl's `debug.log` is one. Write outputs to the container's local disk and copy them to the mounted bucket when the command ends, as the [Axolotl examples](../training/axolotl-finetune.md) do.

## Disk

`disk` is a CLI size string, passed through, such as `500Gi`. When omitted, the request uses `250Gi`.

## Shared memory

`shm` sets `/dev/shm` in the `disk` syntax, such as `64Gi`, or `0`. An integer is taken as bytes. When omitted, a GPU machine gets `16Gi` and a CPU machine sends none, matching the CLI's `--shm-size` default. Raise it for data loaders and multi-GPU training that need more.

## Small files from your machine

`inject_files` takes CLI [`--inject-file`](https://docs.nebius.com/cli/reference/ai/create) strings: `local_path:container_path`. Each local file is read on create, not on `JobSpec` construction, sent as container content, and must be at most 64 KiB. A dry run lists the strings and does not read the files. `inject_files` is not a YAML key.

```python
spec = ns.JobSpec(
    ["python", "/app/run.py"],
    image="python:3.12-slim",
    timeout="30m",
    inject_files=["run.py:/app/run.py"],
)
```

## Working directory

`working_dir` sets the container working directory, at most 128 characters. When omitted, the image `WORKDIR` applies.
