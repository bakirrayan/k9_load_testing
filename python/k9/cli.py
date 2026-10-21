"""Entry point for the ``k9`` console script."""

import signal
import sys
from typing import NoReturn

from k9 import _native


def main() -> NoReturn:
    # The engine handles Ctrl-C itself (the first stops the run and still
    # prints the summary, the second exits), so Python mustn't also turn it
    # into a KeyboardInterrupt.
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    raise SystemExit(_native.cli_main(sys.argv))
