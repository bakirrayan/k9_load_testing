"""The local modules a script imports, so the manager can send them to agents
with the script."""

from __future__ import annotations

import ast
from pathlib import Path


def local_modules(script: str) -> list[str]:
    """The ``.py`` files under the script's directory that it imports,
    directly or through other such files, as paths relative to that
    directory (``helpers.py``, ``lib/__init__.py``). Imports are read, not
    run: modules imported by computed names aren't found."""
    path = Path(script).resolve()
    root = path.parent
    seen = {path}
    todo = [path]
    while todo:
        current = todo.pop()
        try:
            tree = ast.parse(current.read_text(encoding="utf-8"), str(current))
        except (OSError, SyntaxError, UnicodeDecodeError, ValueError):
            continue
        for node in ast.walk(tree):
            for found in _imported_files(node, current, root):
                if found not in seen:
                    seen.add(found)
                    todo.append(found)
    seen.discard(path)
    return sorted(p.relative_to(root).as_posix() for p in seen)


def _imported_files(node: ast.AST, current: Path, root: Path) -> list[Path]:
    """The files under ``root`` that ``node`` (in ``current``) may import."""
    modules: list[tuple[Path, list[str]]] = []
    if isinstance(node, ast.Import):
        modules = [(root, alias.name.split(".")) for alias in node.names]
    elif isinstance(node, ast.ImportFrom):
        base = root
        if node.level:
            base = current.parent
            for _ in range(node.level - 1):
                base = base.parent
        parts = node.module.split(".") if node.module else []
        if parts:
            modules.append((base, parts))
        # `from pkg import name`: name may be a submodule.
        modules += [(base, [*parts, alias.name]) for alias in node.names if alias.name != "*"]
    files = []
    for base, parts in modules:
        for i in range(1, len(parts) + 1):
            here = base.joinpath(*parts[:i])
            candidates = [here / "__init__.py"]
            if i == len(parts):
                candidates.append(here.with_name(here.name + ".py"))
            for candidate in candidates:
                resolved = candidate.resolve()
                if resolved.is_file() and resolved.is_relative_to(root):
                    files.append(resolved)
    return files
