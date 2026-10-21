"""k9's pytest plugin (installed with k9-load, loaded automatically).

A marked test runs a load test, and fails if it crosses thresholds::

    @pytest.mark.k9("load/smoke.py", vus=10, duration="30s")
    def test_checkout_holds_up(k9_result):
        assert k9_result.metric("http_req_duration")["p(95)"] < 300

The marker takes :func:`k9.run`'s arguments; a relative path is relative to
the test file. ``k9_result`` is the marked test's :class:`k9.RunResult`, and
``k9_run`` is :func:`k9.run` with paths relative to the test file, for tests
that run several load tests. Disable with ``-p no:k9``.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest

import k9
from k9._run import thresholds_failed

_RESULT = pytest.StashKey["k9.RunResult"]()


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "k9(target, **options): run this load test (k9.run's arguments) for the "
        "test; crossed thresholds fail it, and k9_result is its result",
    )


def _relative_to(request: pytest.FixtureRequest, target: Any) -> Any:
    if isinstance(target, str | os.PathLike) and not Path(target).is_absolute():
        return request.path.parent / target
    return target


@pytest.fixture
def k9_run(request: pytest.FixtureRequest) -> Callable[..., k9.RunResult]:
    """:func:`k9.run`, with relative paths resolved from the test file."""

    def run(target: Any, **options: Any) -> k9.RunResult:
        return k9.run(_relative_to(request, target), **options)

    return run


@pytest.fixture
def k9_result(request: pytest.FixtureRequest) -> k9.RunResult:
    """The result of the test's ``@pytest.mark.k9(...)`` run."""
    marker = request.node.get_closest_marker("k9")
    if marker is None:
        raise pytest.UsageError("k9_result needs a @pytest.mark.k9(...) marker on the test")
    if not marker.args:
        raise pytest.UsageError("@pytest.mark.k9 needs the script, plan file or k9.plan.Plan")
    options = {"check_thresholds": False, **marker.kwargs}
    result = k9.run(_relative_to(request, marker.args[0]), **options)
    request.node.stash[_RESULT] = result
    return result


@pytest.fixture(autouse=True)
def _k9_marker(request: pytest.FixtureRequest) -> None:
    """Runs a marked test's load test even if it doesn't ask for the result."""
    if request.node.get_closest_marker("k9") is not None:
        request.getfixturevalue("k9_result")


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item) -> Generator[None, None, None]:
    # Crossed thresholds fail the test itself (not its setup), after its body
    # has had its say.
    outcome = yield
    result = item.stash.get(_RESULT, None)
    if result is not None and result.failed_thresholds:
        marker = item.get_closest_marker("k9")
        if marker is None or marker.kwargs.get("check_thresholds", True):
            raise thresholds_failed(result)
    return outcome
