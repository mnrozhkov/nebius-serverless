# YAML and the CLI

## Keep recipes in YAML

A YAML document holds the same recipe as the Python arguments. `JobSpec.from_yaml` and `EndpointSpec.from_yaml` load one; nothing contacts Nebius until `create()`.

=== "Jobs"

    ```yaml title="examples/quickstarts/job_quickstart.yaml"
    --8<-- "examples/quickstarts/job_quickstart.yaml"
    ```

    ```python
    spec = ns.JobSpec.from_yaml("examples/quickstarts/job_quickstart.yaml")
    job = spec.create()
    ```

=== "Endpoints"

    ```yaml title="examples/inference/vllm-endpoint/endpoint.yaml"
    --8<-- "examples/inference/vllm-endpoint/endpoint.yaml"
    ```

    ```python
    spec = ns.EndpointSpec.from_yaml("examples/inference/vllm-endpoint/endpoint.yaml")
    endpoint = spec.create()
    ```

`schema_version: 1` and `kind` are required. Unknown keys, duplicate keys, anchors, tags, merge keys, and a document over 1 MiB are rejected. An endpoint document may not hold a `token`, because YAML files are often committed: use `token_secret`. The allowed keys are under [`JobSpec.from_yaml`](../reference/jobs.md#jobspecfrom_yaml) and [`EndpointSpec.from_yaml`](../reference/endpoints.md#endpointspecfrom_yaml).

## Print the CLI command for a recipe

`spec.to_cli()` returns the `nebius ai job create` (or `nebius ai endpoint create`) command that sends the same request as `create()`, one flag per line. It does not contact Nebius.

```python
print(spec.to_cli())
```

```bash title="examples/quickstarts/job_quickstart.sh"
--8<-- "examples/quickstarts/job_quickstart.sh"
```

Nothing secret is printed: env values become `--env KEY="$KEY"` placeholders for the shell to fill, secrets appear by id, and a token never appears. Without `name=`, a fresh name is generated, as `create()` does. Each argument's flag and API field are in the [CLI and API map](../reference/cli-api-map.md).
