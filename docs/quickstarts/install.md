# Install and authenticate

## Install

Python 3.10 or later. The import name is `nebius_serverless`.

=== "pip"

    ```bash
    pip install nebius-serverless
    ```

=== "uv"

    ```bash
    uv add nebius-serverless
    ```

It depends on `nebius>=0.6.43,<0.7`, `PyYAML>=6`, and `rich>=13`.

## Authenticate

On your machine, use the Nebius CLI's login. Install the [Nebius CLI](https://docs.nebius.com/cli/install), then create a profile:

```bash
nebius profile create
```

The active profile supplies the project id. When the profile does not name a region, `create()` reads it from the project. `profile=`, `project=`, and `region=` select or override them per recipe or per call.

In CI there is no CLI login: use a service account or `NEBIUS_IAM_TOKEN`, as in [Run in CI](../guides/ci.md).

## Check where jobs will go

`ns.whoami()` makes one read-only call and resolves the project and region exactly as `create()` will. It creates nothing.

```python
import nebius_serverless as ns

ns.whoami()
```

It prints one line on stderr, such as:

```text
Signed in as user Ada ada@example.com (useraccount-…) via the active CLI profile. Jobs go to project-… in eu-north1.
```

Credentials that Nebius rejects raise `AccessError` with the request id and what to do next.

## Next

Run your [first job](first-job.md).
