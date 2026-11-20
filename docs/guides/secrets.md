# Secrets and private images

Keep tokens in MysteryBox instead of `env`. The ids below are placeholders.

```python
spec = ns.JobSpec(
    ["python", "train.py", "--config", "config.yaml"],
    image="registry.example.com/team/trainer:1.4",
    gpu="h100",
    timeout="24h",
    pricing="spot",
    spot_policy="pricingpolicy-...",
    restart_attempts=3,
    working_dir="/workspace",
    env={"WANDB_MODE": "offline"},  # (1)!
    secrets={"HF_TOKEN": "mbsec-...", "WANDB_API_KEY": "mbsecver-..."},  # (2)!
    registry_secret="mbsecver-...",  # (3)!
    volumes=["s3://my-data:/workspace/data:ro"],
)
print(spec.create(dry_run=True))  # (4)!
```

1.  Plain values. They are sent on create and left out of dry-run descriptions and exception text. Do not put tokens here.
2.  Env name to MysteryBox id: a secret id (`mbsec-…`, the latest version) or a version id (`mbsecver-…`). The container gets the payload entry whose key matches the name, or the first entry. Names must not repeat an `env` key.
3.  A secret **version** id for a private image registry. Its payload must have `REGISTRY_USERNAME` and `REGISTRY_PASSWORD`. A secret id (`mbsec-…`) raises `ValidationError` here, because the API pins registry credentials to one version.
4.  The description shows secret ids, never values. Secret values never reach the client.

## Endpoints

An endpoint takes the same `working_dir`, `secrets`, `registry_secret`, and `spot_policy`. `token_secret` reads the bearer token from a secret whose payload has `AUTH_TOKEN`, so the token stays the same across creates. `Endpoint.token` is then `None`; clients read the token from MysteryBox.

```python
spec = ns.EndpointSpec(
    image="vllm/vllm-openai:v0.19.1",
    port=8000,
    gpu="h100",
    token_secret="mbsec-...",
    secrets={"HF_TOKEN": "mbsec-..."},
)
```

## In YAML

The same keys work in YAML documents. A `token` key is rejected, because YAML files are often committed: use `token_secret`.

```yaml
schema_version: 1
kind: job
image: registry.example.com/team/trainer:1.4
command: ["python", "train.py"]
gpu: h100
timeout: 24h
pricing: spot
restart_attempts: 3
working_dir: /workspace
secrets:
  HF_TOKEN: mbsec-...
registry_secret: mbsecver-...
```
