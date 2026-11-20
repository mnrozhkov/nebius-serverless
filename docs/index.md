---
hide:
  - navigation
---

# nebius-serverless

Unofficial Python client for Nebius Serverless AI Jobs and Endpoints. Describe a container workload in Python or YAML, create it on a Nebius GPU or CPU, and wait for it from a script, a notebook, or CI.

!!! warning "Unofficial package"
    Not an official Nebius product, and Nebius does not support it. For the official Python SDK, see [github.com/nebius/pysdk](https://github.com/nebius/pysdk). Report problems in this project's [GitHub issues](https://github.com/mnrozhkov/nebius-serverless/issues).

!!! note "Beta"
    0.x releases may change the API.

<div class="grid cards" markdown>

-   :material-rocket-launch-outline: **[Quickstarts](quickstarts/index.md)**

    ---

    Install, then run a first job and a first endpoint.

-   :material-dumbbell: **[Training](training/index.md)**

    ---

    Fine-tune models with Axolotl, one job or a parallel sweep.

-   :material-lightning-bolt-outline: **[Inference](inference/index.md)**

    ---

    Serve a model behind an OpenAI-compatible API.

-   :material-book-open-variant: **[Guides](guides/index.md)**

    ---

    Machines, storage, secrets, retries, CI, YAML and the CLI.

-   :material-code-braces: **[Reference](reference/index.md)**

    ---

    Every class, function, and error.

-   :material-github: **[Source on GitHub](https://github.com/mnrozhkov/nebius-serverless)**

    ---

    The package, the runnable examples, and the issue tracker.

</div>

--8<-- "README.md:install"

--8<-- "README.md:job"

--8<-- "README.md:endpoint"
