"""``k9.metrics``: custom metrics, named and behaving like k6's.

Declare them at module level (k6's init context), then add samples from
iterations and hooks; they appear in the summary and can have thresholds::

    from k9.metrics import Counter, Trend

    orders = Counter("orders")
    checkout_time = Trend("checkout_time", time=True)

    options = {"thresholds": {"checkout_time": ["p(95)<800"]}}

    async def default():
        ...
        orders.add(1, tags={"plan": "pro"})
        checkout_time.add(res.timings.duration)

Samples carry the VU's tags (scenario, plan tags) and the current group.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from k9 import _native
from k9._context import current_group, require_ctx
from k9._errors import MetricError

__all__ = ["Counter", "Gauge", "Metric", "Rate", "Trend"]

#: Metrics declared while the script loads: name → (kind, is_time).
_declared: dict[str, tuple[str, bool]] = {}
#: Set once the script has loaded; declaring a metric after that fails.
_frozen = False


def _reset() -> None:
    global _frozen
    _declared.clear()
    _frozen = False


def _freeze() -> list[tuple[str, str, bool]]:
    global _frozen
    _frozen = True
    return [(name, kind, is_time) for name, (kind, is_time) in _declared.items()]


class Metric:
    """A custom metric. Use one of the subclasses."""

    kind: ClassVar[str]

    def __init__(self, name: str, time: bool = False) -> None:
        """``time=True`` marks values as milliseconds, for display (k6's ``isTime``)."""
        if _frozen:
            raise MetricError(
                f"metric {name!r} must be declared at module level, before the test starts"
            )
        problem = _native.metric_error(name, self.kind)
        if problem is not None:
            raise MetricError(problem)
        existing = _declared.get(name)
        if existing is not None and existing[0] != self.kind:
            raise MetricError(f"metric `{name}` is already declared as a {existing[0]}")
        _declared[name] = (self.kind, time)
        self.name = name
        self.is_time = time

    def _add(self, value: float, tags: Mapping[str, str] | None) -> None:
        ctx = require_ctx(f"{type(self).__name__}.add()")
        tag_list = [(str(k), str(v)) for k, v in tags.items()] if tags else []
        ctx.add(self.name, float(value), current_group.get(), tag_list)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"


class Counter(Metric):
    """Sums what's added; the summary shows the total and the rate per second."""

    kind = "counter"

    def add(self, value: float = 1, tags: Mapping[str, str] | None = None) -> None:
        self._add(value, tags)


class Gauge(Metric):
    """Keeps the latest value added, plus min and max."""

    kind = "gauge"

    def add(self, value: float, tags: Mapping[str, str] | None = None) -> None:
        self._add(value, tags)


class Rate(Metric):
    """The share of added values that are true (non-zero)."""

    kind = "rate"

    def add(self, value: bool | float, tags: Mapping[str, str] | None = None) -> None:
        self._add(float(bool(value)), tags)


class Trend(Metric):
    """A distribution: the summary shows avg, min, med, max, p(90) and p(95)."""

    kind = "trend"

    def add(self, value: float, tags: Mapping[str, str] | None = None) -> None:
        self._add(value, tags)
