# Batch: GPU matrix multiplication

Runs three independent GPU jobs, one matrix size each (`1024`, `2048`, `4096`), then collects and checks one JSON report per job. It shows the batch pattern: submit every job first, then wait for each.

## What this example does

- [`matrix_multiply.py`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/batch/matrix-multiply/matrix_multiply.py) creates all three jobs and prints each id as soon as create is accepted. Then it waits for each job and reads its report from Object Storage.
- Each job multiplies two random square matrices on the GPU in float32. The worker turns off TF32 (`torch.backends.cuda.matmul.allow_tf32 = False`, `torch.set_float32_matmul_precision("highest")`), checks a 2×2 product, then checks the shape, finiteness, and nine sampled entries of the large product. A failed check writes no success report.
- Each report is `/data/<prefix>/<size>.json` with `schema_version` 1, `size`, `gpu`, `elapsed_ms`, `mean`, and `verified: true`.
- There is no `.map()`. The jobs may run at the same time; the script does not promise that they start together, or that the batch ends sooner than the same jobs one after another.

## Requirements

- A Nebius profile that can create jobs in a region where `gpu="h100"` exists: see [Install and authenticate](https://mnrozhkov.github.io/nebius-serverless/quickstarts/install/).
- Quota for up to three one-GPU H100 jobs at once.
- A bucket the jobs can mount as `s3://<bucket>:/data:rw`. Set `BUCKET` in the script; it is the placeholder `my-bucket`. Change `ENDPOINT` when the bucket is not in `eu-north1`.
- The AWS CLI (v2) with credentials that can read the report objects, and delete them when you clean up. No input files are uploaded, and the controller does not import PyTorch.

## Runtime and compute

| | |
| --- | --- |
| Jobs | 3, one per size |
| Machine | `gpu="h100"` each, on-demand |
| Timeout | `1h` each, the remote deadline |
| Local wait | `wait(timeout=5400)`: stops the local poll only, never cancels |
| Image | `pytorch/pytorch@sha256:f894dae26e1ee8557c544f9cfdb9dc011b1552bf3c1e656b422f2e221d380e40` |

The image is pinned by digest: the `linux/amd64` manifest of `pytorch/pytorch:2.6.0-cuda12.6-cudnn9-runtime`, with PyTorch 2.6.0 (`2.6.0+cu126`), Python 3.11.11, CUDA 12.6.3, cuDNN 9, and Ubuntu 22.04.

## Run it

```bash
uv run python examples/batch/matrix-multiply/matrix_multiply.py
```

## Expected output

The object prefix and each job id as soon as create is accepted, then a tab-separated table. The numbers below are an illustration; timings, means, and the GPU name are not fixed results. On a real H100 the `gpu` column names an H100; another device means the allocation did not match `gpu="h100"`.

```text
prefix matrix-multiply/0124abcd0124abcd0124abcd0124abcd
size 1024: bt1example1024
size 2048: bt1example2048
size 4096: bt1example4096
size    gpu                     elapsed_ms    mean      verified
1024    NVIDIA H100 80GB HBM3   12.5          0.0011    True
2048    NVIDIA H100 80GB HBM3   48.2          -0.0003   True
4096    NVIDIA H100 80GB HBM3   190.4         0.0004    True
```

After `COMPLETED`, the script polls for a missing report for at most 60 seconds, every 2 seconds. An authentication failure, malformed JSON, or an invalid report stops it at once.

## How to adapt it

- Change `SIZES` for other inputs: one job per entry.
- Replace the embedded worker with your own command; keep the pattern of creating every job before the first `wait()`.

## Troubleshooting

An error stops the script and names the size and, once create has returned, the job id. Jobs already accepted keep running: nothing is resubmitted, cancelled, or deleted, and a local wait timeout does not stop a container. Clean up by hand, only this run's ids and prefix:

```bash
nebius ai job delete <id>
aws s3 rm --recursive "s3://<bucket>/matrix-multiply/<uuid>/" \
  --endpoint-url https://storage.eu-north1.nebius.cloud
```
