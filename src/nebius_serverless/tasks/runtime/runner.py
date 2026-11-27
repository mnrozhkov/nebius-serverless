"""Run one ns.Task function inside the job."""

import hashlib
import importlib
import io
import json
import os
import sys
import tarfile
import traceback


def load(payload, root):
    if payload["libs"]:
        folder = payload["libs_dir"]
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(root, payload["libs"]), "rb") as handle:
            data = handle.read()
        expected = os.path.basename(payload["libs"]).split(".")[0]
        if hashlib.sha256(data).hexdigest()[: len(expected)] != expected:
            raise RuntimeError("The library archive is not what was sent.")
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            try:
                archive.extractall(folder, filter="data")
            except TypeError:
                archive.extractall(folder)
        sys.path.insert(0, folder)
        previous = os.environ.get("PYTHONPATH")
        os.environ["PYTHONPATH"] = (
            folder + os.pathsep + previous if previous else folder
        )
    if payload["module"]:
        target = importlib.import_module(payload["module"])
        for part in payload["qualname"].split("."):
            target = getattr(target, part)
        return target
    scope = {"__name__": "__ns_task__"}
    exec(compile(payload["source"], "<ns.Task source>", "exec"), scope)
    return scope[payload["qualname"]]


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "pointer.json")) as handle:
        pointer = json.load(handle)
    root = pointer["root"]
    run = os.path.join(root, pointer["run"])
    # Which attempt of the run this job is: 1, then 2 after a preemption.
    attempt = int(pointer.get("attempt", 1))
    files = os.path.join(run, "files")
    os.makedirs(files, exist_ok=True)
    os.environ["NS_OUTPUT_DIR"] = files
    os.environ["NS_TASK_MOUNT"] = root
    os.environ["NS_TASK_ATTEMPT"] = str(attempt)
    code = 0
    try:
        with open(os.path.join(run, "payload.json"), "rb") as handle:
            raw = handle.read()
        if hashlib.sha256(raw).hexdigest() != pointer["sha256"]:
            raise RuntimeError("payload.json is not what was submitted.")
        payload = json.loads(raw)
        function = load(payload, root)
        value = function(*payload["args"], **payload["kwargs"])
        body = json.dumps({"ok": True, "value": value, "attempt": attempt})
    except BaseException as exc:
        code = 1
        traceback.print_exc()
        error = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        body = json.dumps({"ok": False, "error": error, "attempt": attempt})
    with open(os.path.join(run, "result.json"), "w") as handle:
        handle.write(body)
    sys.exit(code)


if __name__ == "__main__":
    main()
