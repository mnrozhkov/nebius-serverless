# Images

`ns.Image` is a Dockerfile that Nebius builds for you. `build()` runs the build on a CPU serverless job, pushes the image to a Container Registry in your project, and returns a reference for `JobSpec(image=...)`, `EndpointSpec(image=...)`, or `Task(image=...)`. There is no Docker on your machine and no registry setup.

## Image

```python
ns.Image(dockerfile=None, *, dockerfile_string=None)
```

| Parameter | Meaning |
| --- | --- |
| `dockerfile` | Path to a local Dockerfile, at most 64 KiB. The image's name in the registry is the Dockerfile's folder name. |
| `dockerfile_string` | The Dockerfile text, at most 64 KiB. The image's name is `image`. |

Pass exactly one of the two. Constructing an `Image` checks the Dockerfile and does not contact Nebius.

After construction these attributes are readable: `dockerfile` (a resolved `Path`, or `None`), `dockerfile_string`, `name`, and `tag`.

`tag` is `sha-` plus the first 12 hex characters of the Dockerfile's SHA-256. A path is read again each time, so an edited Dockerfile gets a new tag.

### `Image.build`

```python
image.build(dry_run=False, *, timeout="1h", disk=None, quiet=False) -> str
```

Make sure the image is in the registry, and return its reference: `cr.<region>.nebius.cloud/<registry>/<name>:<tag>`.

1. Finds the project's `generated-images` registry, and creates it on the first build.
2. If the registry already has this tag, prints `Reusing ...` and returns. A Dockerfile is built once.
3. Otherwise stores a short-lived push token as a new version of the `generated-images-push` MysteryBox secret, runs [kaniko](https://github.com/GoogleContainerTools/kaniko) on a CPU job, waits for it, and deletes that secret version. A first build takes about two minutes. `RUN` layers are cached in the registry, so a small edit rebuilds only the changed steps.

| Parameter | Meaning |
| --- | --- |
| `dry_run` | `True` creates nothing and returns a dict: the reference (`image`), whether it would be reused (`reuse`), the registry, and the build job request (`job`, `None` on reuse). |
| `timeout` | The build job's deadline, as on `JobSpec`. Default one hour. |
| `disk` | The build job's disk, as on `JobSpec`. |
| `quiet` | `True` suppresses progress for this call. |

A build that does not end `COMPLETED` raises `BuildFailed`. Its `resource_id` is the build job: `nebius ai job logs <id>` shows why.

Jobs in the same project pull the image without credentials. Project and region come from `ns.configure` or the active CLI profile.

### `Image.build_async`

The awaited twin of `build`, with the same arguments. Cancelling the await does not cancel a running build job.

## Limits

- The build context is the Dockerfile alone. `COPY` of local files is not supported yet; `COPY --from=<image>` works.
- `RUN` steps share the build container with the push token, so build only Dockerfiles you trust.

See [First image](../quickstarts/first-image.md) for a runnable example.
