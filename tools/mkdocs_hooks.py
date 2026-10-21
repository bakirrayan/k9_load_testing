"""MkDocs hook: links from the docs to files elsewhere in the repository
(``../examples/script.py``) point at the repository on GitHub when
``repo_url`` is set in mkdocs.yml, and are kept as plain text until then."""

from __future__ import annotations

import posixpath
import re
from typing import Any

LINK = re.compile(r"(?<!!)\[([^\]]+)\]\(([^)\s#]+)(#[^)\s]*)?\)")


def on_page_markdown(markdown: str, page: Any, config: Any, files: Any) -> str:
    base = posixpath.dirname(page.file.src_uri)
    repo = (config.repo_url or "").rstrip("/")

    def fix(match: re.Match[str]) -> str:
        text, target, anchor = match.group(1), match.group(2), match.group(3) or ""
        if ":" in target:
            return match.group(0)
        path = posixpath.normpath(posixpath.join("docs", base, target))
        if path.startswith("docs/"):
            return match.group(0)
        if not repo:
            return text
        kind = "tree" if target.endswith("/") else "blob"
        return f"[{text}]({repo}/{kind}/main/{path}{anchor})"

    return LINK.sub(fix, markdown)
