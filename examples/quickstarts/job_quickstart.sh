#!/usr/bin/env bash
# The job from job_quickstart.py as a Nebius CLI command: the output of
# ns.Job(spec).to_cli() for that spec, with a fixed --name. A test keeps the
# two in step.
set -euo pipefail

nebius ai job create \
  --name nvidia-smi-quickstart \
  --image nvidia/cuda:13.1.1-runtime-ubuntu24.04 \
  --timeout 1h \
  --container-command nvidia-smi \
  --gpu h100 \
  --disk-size 250Gi \
  --shm-size 16Gi
