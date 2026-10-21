# Writing tests

## Starting out: `k9 new`

`k9 new` writes a starter that runs as is. It requests one URL with a check,
then sleeps, with 10 VUs for 30s and thresholds on errors and p(95).

```sh
k9 new                                  # script.py: async def default() (Tier 3)
k9 new load.yaml                        # a declarative plan (Tier 1)
k9 new load.py --template plan          # a k9.plan script (Tier 1)
k9 new load.py --template sync          # def default(), a thread per VU (Tier 3)
k9 new --url https://staging.example.com/health
```

The template comes from the file's extension unless `--template` is given;
the default URL is the test server's `/ok`. Python scripts also read the URL
from `-e TARGET=…`. An existing file is only replaced with `--force`.

## From a recording: `k9 convert`

`k9 convert` turns a HAR file (saved from a browser's developer tools or a
proxy) into a plan. Each page becomes a group, and a pause between requests
becomes a `sleep`. Each request gets a check on the status it was recorded
with. Headers the client sets itself are left out. So are redirects k9
follows, and cookies a response set, since the VU's cookie jar sends those.

```sh
k9 convert login.har -O login.yaml --only shop.example.com --no-static
k9 run login.yaml
```

Values a response handed out and later requests sent back are **correlated**.
Each one is taken from that response with a `json`, `header` or `regex`
extractor and sent as `{{ variable }}`, so each run uses its own token. This
covers tokens, session and request ids, and CSRF fields in HTML. A value
counts when it's at least 8 characters long and is either opaque (letters and
digits) or under a name like `token`, `session`, `csrf` or `…id`, and the
client didn't send it before the server did. `--no-correlate` keeps the
recorded values. `--skip HOST` leaves a host out (it takes subdomains too, like
`--only`).
