"""Code that travels with a task: ``local_libs`` and function source.

A package listed in ``local_libs`` is packed into one archive, the
same bytes for the same files, and uploaded once per change. A function
from anywhere else is sent as its source, checked to stand on its own.
"""

import ast
import builtins
import dis
import gzip
import hashlib
import importlib.util
import inspect
import io
import re
import sysconfig
import tarfile
import textwrap
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import CodeType
from typing import Any

from .errors import ValidationError
from .output import Progress, report
from .status import Status
from .task_storage import Bucket, SdkTaskBackend

_OP = "Task"

_LIB_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


@dataclass(frozen=True)
class Libs:
    """The local_libs archive, its content hash, and its file count."""

    data: bytes
    digest: str
    count: int


def validate_libs(names: object) -> tuple[str, ...]:
    if isinstance(names, str) or not isinstance(names, Sequence):
        raise ValidationError(
            'local_libs must be a list of import names, such as ["tasks"].',
            operation=_OP,
        )
    for name in names:
        if not isinstance(name, str) or not _LIB_NAME.fullmatch(name):
            raise ValidationError(
                f"local_libs takes import names such as 'tasks' or "
                f"'tasks.training', not {name!r}.",
                operation=_OP,
            )
        _lib_files(name)
    return tuple(names)


def covered(module: str, libs: Sequence[str]) -> bool:
    return any(
        module == name or module.startswith(name + ".") for name in libs
    )


def _lib_files(name: str) -> list[tuple[str, Path]]:
    """The files one import name stands for, as ``(archive name, path)``.

    The top-level name is found the way an import finds it, and the
    rest is walked on disk, so the library's code is not run.
    """
    top, *rest = name.split(".")
    try:
        spec = importlib.util.find_spec(top)
    except (ImportError, ValueError):
        spec = None
    if spec is None:
        raise ValidationError(
            f"local_libs: {top!r} cannot be imported here. Run from "
            "the folder that contains it, or add that folder to "
            "sys.path.",
            operation=_OP,
        )
    if spec.submodule_search_locations:
        current = Path(list(spec.submodule_search_locations)[0])
    elif spec.origin and spec.origin.endswith(".py"):
        current = Path(spec.origin)
    else:
        raise ValidationError(
            f"local_libs: {top!r} is not a local Python package or module.",
            operation=_OP,
        )
    current = current.resolve()
    if _installed(current):
        raise ValidationError(
            f"local_libs: {top!r} is an installed package. Install it "
            "in the image instead.",
            operation=_OP,
        )
    base = current.parent
    files: list[Path] = []
    for part in rest:
        if not current.is_dir():
            raise ValidationError(
                f"local_libs: {name!r} was not found.", operation=_OP
            )
        init = current / "__init__.py"
        if init.is_file():
            files.append(init)
        folder = current / part
        current = folder if folder.is_dir() else folder.with_suffix(".py")
    if current.is_dir():
        files.extend(
            path
            for path in sorted(current.rglob("*"))
            if path.is_file() and sendable(path.relative_to(current))
        )
    elif current.is_file():
        files.append(current)
    else:
        raise ValidationError(
            f"local_libs: {name!r} was not found.", operation=_OP
        )
    return [(path.relative_to(base).as_posix(), path) for path in files]


def _installed(path: Path) -> bool:
    parts = set(path.parts)
    if "site-packages" in parts or "dist-packages" in parts:
        return True
    stdlib = sysconfig.get_paths().get("stdlib")
    return bool(stdlib) and Path(str(stdlib)).resolve() in path.parents


def sendable(relative: Path) -> bool:
    if relative.suffix in (".pyc", ".pyo"):
        return False
    return not any(
        part == "__pycache__" or part.startswith(".")
        for part in relative.parts
    )


def pack_libs(names: Sequence[str]) -> Libs | None:
    """One tar.gz of the libs, the same bytes for the same files."""
    if not names:
        return None
    files: dict[str, Path] = {}
    for name in names:
        files.update(_lib_files(name))
    return pack_files(files)


def pack_files(files: Mapping[str, Path]) -> Libs:
    """One tar.gz of ``archive name: path``, in a fixed order."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as zipped:
        with tarfile.open(fileobj=zipped, mode="w") as archive:
            for arcname in sorted(files):
                content = files[arcname].read_bytes()
                info = tarfile.TarInfo(arcname)
                info.size = len(content)
                info.mode = 0o644
                archive.addfile(info, io.BytesIO(content))
    data = buffer.getvalue()
    digest = hashlib.sha256(data).hexdigest()[:16]
    return Libs(data=data, digest=digest, count=len(files))


def upload_libs(
    libs: "Libs | None",
    backend: SdkTaskBackend,
    bucket: Bucket,
    progress: Progress,
    what: str = "local_libs",
) -> str | None:
    """Upload an archive unless the bucket has it. Returns its key."""
    if libs is None:
        return None
    key = f"libs/{libs.digest}.tar.gz"
    if not backend.exists(bucket, key):
        backend.put(bucket, key, libs.data)
        _say(progress, f"Uploaded {what} ({libs.count} files).")
    return key


def function_source(
    function: Callable[..., Any],
    *,
    allowed: Iterable[str] = (),
    strip_decorators: bool = False,
) -> str:
    """The function's source, checked to stand on its own in the job.

    ``allowed`` names from the surrounding file are let through; the
    caller sends them another way. ``strip_decorators`` drops the
    decorators instead of refusing them.
    """
    permitted = set(allowed)
    name = function.__name__
    try:
        text = textwrap.dedent(inspect.getsource(function))
        tree = ast.parse(text)
    except (OSError, TypeError, SyntaxError):
        raise ValidationError(
            f"The source of {name} cannot be read, so it cannot be "
            "sent. Define it in a file, or put it in a package listed "
            "in local_libs.",
            operation=_OP,
        ) from None
    node = tree.body[0] if tree.body else None
    if not isinstance(node, ast.FunctionDef):
        raise ValidationError(
            f"{name} must be defined with def to be sent as source.",
            operation=_OP,
        )
    if node.decorator_list and strip_decorators:
        node.decorator_list = []
    if node.decorator_list:
        raise ValidationError(
            f"{name} has a decorator, which cannot be sent with it. "
            "Put it in a package listed in local_libs.",
            operation=_OP,
        )
    if getattr(function, "__closure__", None):
        raise ValidationError(
            f"{name} uses variables of an enclosing function, which "
            "are not sent with it.",
            operation=_OP,
        )
    code = getattr(function, "__code__", None)
    outside = sorted(
        item
        for item in (globals_used(code) if code is not None else ())
        if not hasattr(builtins, item)
        and item != name
        and item not in permitted
    )
    if outside:
        raise ValidationError(
            f"{name} uses names from the surrounding file: "
            f"{', '.join(outside)}. Its source is sent on its own, so "
            "put imports and helpers inside the function, or put the "
            "function in a package listed in local_libs.",
            operation=_OP,
        )
    # Annotations may name things the job does not have.
    node.returns = None
    arguments = node.args
    for argument in (
        *arguments.posonlyargs,
        *arguments.args,
        *arguments.kwonlyargs,
        arguments.vararg,
        arguments.kwarg,
    ):
        if argument is not None:
            argument.annotation = None
    return ast.unparse(node) + "\n"


def globals_used(code: CodeType) -> set[str]:
    names = {
        str(instruction.argval)
        for instruction in dis.get_instructions(code)
        if instruction.opname == "LOAD_GLOBAL"
    }
    for constant in code.co_consts:
        if isinstance(constant, CodeType):
            names |= globals_used(constant)
    return names


def _say(progress: Progress, message: str) -> None:
    report(Status(kind="task", message=message), progress)
