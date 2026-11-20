# Parallel fine-tuning sweep

Fine-tunes two models at the same time, one spot H100 job each, and saves each result to its own folder in one bucket.

## What this example does

- [`sweep.py`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/training/axolotl-sweep/sweep.py) builds two `JobSpec` recipes, creates both jobs, then waits for each and prints its record. The second job is created before the first one is waited on, so they can run in parallel.
- [`configs/qwen_0_5b.yaml`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/training/axolotl-sweep/configs/qwen_0_5b.yaml): QLoRA for `Qwen/Qwen2.5-0.5B`, `learning_rate: 2e-4`, `lora_r: 8`.
- [`configs/qwen_1_5b.yaml`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/training/axolotl-sweep/configs/qwen_1_5b.yaml): QLoRA for `Qwen/Qwen2.5-1.5B`, `learning_rate: 1e-4`, `lora_r: 16`.
- Both jobs mount `s3://fine-tuning-axolotl` at `/workspace/data`, train from `/workspace/data/configs/…`, and copy their output to `s3://fine-tuning-axolotl/output/qwen_0_5b/` and `s3://fine-tuning-axolotl/output/qwen_1_5b/`.

## Requirements

- A Nebius profile that can create jobs: see [Install and authenticate](https://mnrozhkov.github.io/nebius-serverless/quickstarts/install/).
- Spot quota for two H100 jobs at the same time.
- An Object Storage bucket named `fine-tuning-axolotl` that the jobs can mount read-write, and the AWS CLI with credentials for it.

## Runtime and compute

| | |
| --- | --- |
| Jobs | 2, named `axolotl-qwen-0.5b` and `axolotl-qwen-1.5b` |
| Machine | `gpu="h100"` each, `pricing="spot"` |
| Disk | `500Gi` each |
| Timeout | `24h` each |

## Run it

Upload both configs, then start the sweep from the repository root:

```bash
aws s3 cp examples/training/axolotl-sweep/configs/ \
  s3://fine-tuning-axolotl/configs/ --recursive \
  --endpoint-url https://storage.eu-north1.nebius.cloud
uv run python examples/training/axolotl-sweep/sweep.py
```

## Expected output

Two job ids, one per `create()`. Then, for each job in turn, the state changes and the final record from `display()`. The first wait can end long after the second job finished: waiting is sequential, the jobs are not.

## How to adapt it

- Add a model: add a config under `configs/`, and a `JobSpec` with its own output folder and name.
- Generate the recipes in a loop when they differ only in the config path; each `create()` gets its own job.
- Use [Run many jobs at once](https://mnrozhkov.github.io/nebius-serverless/guides/many-jobs/) for the pattern with error handling and result checks.

## Troubleshooting

- **Writes to `debug.log` fail on the bucket.** Both configs keep `output_dir: /workspace/output` on the local disk, because Axolotl's appends to `debug.log` are not supported on the S3 mount. Keep it that way in new configs.
- **One job waits in `PROVISIONING`, or ends in `ERROR`.** The project may not have spot capacity or quota for two H100s at once. The final record shows the reason.
