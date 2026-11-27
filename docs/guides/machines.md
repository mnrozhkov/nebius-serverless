# Choose a machine and pricing

## Machine selection

`gpu` and `cpu` are the API's machine selectors, sent as they are. Nebius picks the platform and preset in the project's region at create time, and reads (`get`, `wait`, `display`) show the platform and preset it chose.

| Argument | Example | Sent as | Machine |
| --- | --- | --- | --- |
| `gpu="MODEL"` | `gpu="h100"` | `spec.gpu = ["h100"]` | One GPU of that model |
| `gpu="MODEL:COUNT"` | `gpu="h100:8"` | `spec.gpu = ["h100:8"]` | COUNT GPUs, at least 1 |
| `cpu="FAMILY:VCPUS"` | `cpu="amd:16"` | `spec.cpu = ["amd:16"]` | A CPU machine with that many vCPUs |
| `platform=`, `preset=` | `platform="gpu-h100-sxm"` | `platform`, `preset` | Exactly that platform and preset |
| none of them | | `platform="cpu-d3"`, `preset="2vcpu-8gb"` | The smallest CPU machine |

In `eu-north1`, `gpu="h100"` resolves to the platform `gpu-h100-sxm` and the preset `1gpu-16vcpu-200gb`, which `job.display()` shows.

The client checks only the shape (`h100:0`, `:8` and `gpu="cpu"` raise `ValidationError`). `gpu` and `cpu` exclude each other, and either excludes `platform` and `preset`. A model, count or family that the region does not offer is Nebius's call: create raises `ValidationError` with the code and Nebius's reason, or `CapacityError`. `platform=` without `preset=` fills the smallest preset for known platforms; the CLI marks `--platform`/`--preset` "will be deprecated soon". Spot pricing on a CPU machine (`cpu=`, a `cpu-…` platform, or no machine argument) raises `UnsupportedFeature`.

## On-demand or spot

`pricing="on-demand"` is the default. `pricing="spot"` is preemptible and follows the current spot price: cheaper, but Nebius can stop the machine. `"spot"` on CPU raises `UnsupportedFeature`.

```python
spec = ns.JobSpec(
    ["python", "train.py"],
    image="registry.example.com/team/trainer:1.4",
    gpu="h100",
    timeout="24h",
    pricing="spot",
    spot_policy="pricingpolicy-...",  # optional: cap the price
)
```

`spot_policy` is a pricing policy id that caps the spot price. It requires `pricing="spot"`, and is sent as `pricing_model.spot_pricing_policy` in place of `follows_spot_price`: the machine is preempted when the spot price exceeds the cap.

## Restarts and preemption

`restart_attempts` restarts the container on the same machine after the command fails: `N` allows up to `N` restarts after the first run, and `-1` restarts without limit. A successful exit is never restarted.

Nebius never restarts after preemption, lack of capacity, an unexpected machine stop, a startup failure, or a timeout: the job ends in `ERROR`. For a spot job, this client creates the job again when `wait()` sees it preempted, up to `preemption_retries` times. Without `preemption_retries`, the number comes from `restart_attempts` (10 for `-1`). See [Spot preemption](../reference/jobs.md#spot-preemption).

!!! warning "Resubmission happens only while `wait()` runs"
    If the script or notebook stops, nothing is resubmitted. Each attempt starts over with the full `timeout`, so save checkpoints to a mounted volume and resume from them.

A preempted spot endpoint goes to `STOPPED` with the code `Preempted`; `start()` resumes it.
