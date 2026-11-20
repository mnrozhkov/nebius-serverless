# Fine-tune a model with Axolotl

Fine-tunes `Qwen/Qwen2.5-0.5B` with QLoRA on one spot H100, then copies the checkpoints and adapter to Object Storage. It is the Python version of the [Serverless AI Cookbook Axolotl recipe](https://github.com/nebius/serverless-ai-cookbook/blob/main/training/axolotl-finetuning/README.md).

## What this example does

- [`finetune.py`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/training/axolotl-finetune/finetune.py) creates one job running `axolotl train` in `docker.io/axolotlai/axolotl:main-20260309-py3.11-cu128-2.9.1`, waits for it, and prints the final record.
- The job mounts the bucket `fine-tuning-axolotl` at `/workspace/data`, where Axolotl reads [`config.yaml`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/training/axolotl-finetune/config.yaml): `Qwen/Qwen2.5-0.5B`, QLoRA, `Salesforce/wikitext`.
- Training writes to `/workspace/output` on the container's local disk. When it succeeds, the job copies that folder to `/workspace/data/output/run-<date>-<time>/`, which is `s3://fine-tuning-axolotl/output/run-<date>-<time>/`.

## Requirements

- A Nebius profile that can create jobs: see [Install and authenticate](https://mnrozhkov.github.io/nebius-serverless/quickstarts/install/).
- Spot H100 quota in the project's region.
- An Object Storage bucket named `fine-tuning-axolotl` that the job can mount read-write, and the AWS CLI with credentials for it.

## Runtime and compute

| | |
| --- | --- |
| Machine | `gpu="h100"`: one H100, `pricing="spot"` |
| Disk | `500Gi` |
| Timeout | `24h` |
| Shared memory | `16Gi`, the GPU default |

## Run it

Upload the config, then create the job from the repository root:

```bash
aws s3 cp examples/training/axolotl-finetune/config.yaml \
  s3://fine-tuning-axolotl/config.yaml \
  --endpoint-url https://storage.eu-north1.nebius.cloud
uv run python examples/training/axolotl-finetune/finetune.py
```

Read the container logs while it runs or after it ends:

```bash
nebius ai job logs <job-id>
```

## Expected output

`create()` prints the job id. `wait()` prints each new state until the job ends, and `display()` prints the final record: machine, disk, timings, and the state. On `COMPLETED` the adapter and checkpoints are in `s3://fine-tuning-axolotl/output/run-<date>-<time>/`.

## How to adapt it

- Change the model, dataset, or LoRA settings in `config.yaml`, then upload it again.
- Ask for more GPUs with `gpu="h100:8"`, and match Axolotl's settings to them.
- Raise shared memory with `shm="64Gi"`.
- Drop `pricing="spot"` for an on-demand machine that is not preempted.
- Set `restart_attempts=2` to have `wait()` create the job again after a spot preemption. Each attempt starts over: see [Spot preemption](https://mnrozhkov.github.io/nebius-serverless/reference/jobs/#spot-preemption).
- Train several configs at once: see the [parallel sweep](https://mnrozhkov.github.io/nebius-serverless/training/axolotl-sweep/).

## Troubleshooting

- **Writes to `debug.log` fail on the bucket.** Axolotl appends to `debug.log` in its `output_dir`, and the S3 mount does not support that. Keep `output_dir: /workspace/output` on the local disk, as `config.yaml` does, and copy the result after training.
- **The job ends in `ERROR`.** The final `wait()` line and `display()` show the reason in `status.state_details`, for example `NotEnoughResources` or `Preempted`. Try again, switch to on-demand, or set `restart_attempts`.
- **`CapacityError` on create.** The project's quota is too small. The message names the quota, the amount requested, and the limit.
