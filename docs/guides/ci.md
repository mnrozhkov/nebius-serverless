# Run in CI

Use a service account. Create an authorized key for it, then store the credentials file as a CI secret:

```yaml
# .github/workflows/train.yml
jobs:
  train:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install nebius-serverless
      - run: echo "$NEBIUS_SA_CREDENTIALS" > "$RUNNER_TEMP/sa.json"
        env:
          NEBIUS_SA_CREDENTIALS: ${{ secrets.NEBIUS_SA_CREDENTIALS }}
      - run: python train.py
        env:
          NEBIUS_CREDENTIALS_FILE: ${{ runner.temp }}/sa.json
          NEBIUS_PROJECT: ${{ vars.NEBIUS_PROJECT }}
```

```python
# train.py
import os

import nebius_serverless as ns

ns.configure(
    credentials_file=os.environ["NEBIUS_CREDENTIALS_FILE"],
    project=os.environ["NEBIUS_PROJECT"],
)
job = ns.JobSpec(["python", "train.py"], image="…", gpu="h100", timeout="2h").create()
job.wait()
```

A short-lived token works too: set `NEBIUS_IAM_TOKEN` and pass `project=`; no `configure` call is needed.

Without a CLI config file there is no profile to read a project from, so `create()` raises `ValidationError` until `project=` or `configure(project=…)` names one. Add `ns.whoami()` as a first step: it checks the credentials and prints where jobs would go, without creating anything.

The order in which credentials are picked up is under [`configure`](../reference/session.md#configure).
