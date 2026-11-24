# First image

Build a container image from a Dockerfile on Nebius, then run a job on it. There is no Docker on your machine and no registry setup:

```python title="examples/quickstarts/image_quickstart.py"
--8<-- "examples/quickstarts/image_quickstart.py"
```

```bash
uv run python examples/quickstarts/image_quickstart.py
```

## What happens

1. `ns.Image(dockerfile_string=...)` holds the Dockerfile. Pass a path instead with `ns.Image("./Dockerfile")`. Nothing contacts Nebius yet.
2. `build()` looks for the project's `generated-images` Container Registry and creates it on the first build.
3. The tag is `sha-` plus a hash of the Dockerfile. If the registry already has that tag, `build()` prints `Reusing ...` and returns at once. A Dockerfile is built once, and an edited one gets a new tag.
4. Otherwise `build()` runs [kaniko](https://github.com/GoogleContainerTools/kaniko) on a CPU job, waits for it, and prints `Pushed ...`. A first build takes about two minutes.
5. `build()` returns the image reference, `cr.<region>.nebius.cloud/<registry>/<name>:<tag>`. Jobs in the same project pull it without credentials.

A build that does not end `COMPLETED` raises `ns.BuildFailed`; its `resource_id` is the build job, and `nebius ai job logs <id>` shows why. `build(dry_run=True)` returns the reference, whether it would be reused, and the build job request, and creates nothing.

## Credentials

The push uses your own credentials, the same ones that create jobs. Each build stores a short-lived access token as a new version of the `generated-images-push` MysteryBox secret, passes it to the build job, and deletes that version when the build ends. `RUN` steps share the build container, so build only Dockerfiles you trust.

## Limits

- The build context is the Dockerfile alone. `COPY` of local files is not supported yet; `COPY --from=<image>` works.
- The Dockerfile must be at most 64 KiB.

## Next

Use the image in a [training](../training/index.md) job, or step through a GPU build in the [image notebook](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/images/image.ipynb).
