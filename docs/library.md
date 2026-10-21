# Library and pytest

`k9.run()` runs a script, a plan file or a `k9.plan.Plan` (in a child process)
and returns a `RunResult`:

```python
import k9

result = k9.run("load/smoke.py", vus=20, duration="30s", env={"BASE": base_url})
print(result.iterations, result.metric("http_req_duration")["p(95)"])
```

Trends in a `RunResult` always have `count`, `avg`, `min`, `med`, `max`, `p(90)`,
`p(95)`, `p(99)` and `p(99.9)`, plus any `summary_trend_stats` asks for.

Crossed thresholds raise `k9.ThresholdsFailed` (an `AssertionError`); pass
`check_thresholds=False` to get the result anyway. A test that can't start raises
`k9.ConfigError` or `k9.ScriptError`. All three are `k9.RunError`s, with the exit
code, stderr and, if the run finished, its result. `summary_trend_stats=["p(99)"]`
adds percentiles beyond the default p(90) and p(95).

Installing k9-load also installs a pytest plugin. The marker runs the load test,
and crossed thresholds fail the test:

```python
import pytest

@pytest.mark.k9("load/smoke.py", vus=10, duration="30s")  # relative to this file
def test_checkout_holds_up(k9_result):
    assert k9_result.metric("http_req_duration")["p(95)"] < 300

def test_two_runs(k9_run):
    assert k9_run("load/a.yaml").ok and k9_run("load/b.yaml").ok
```
