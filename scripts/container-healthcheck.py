#!/usr/bin/env python3
"""Map the public health envelope to Docker's healthcheck exit contract."""

from __future__ import annotations

import json
import subprocess
import sys


def main() -> int:
    completed = subprocess.run(
        ["/usr/local/bin/gap", "health", "--json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=18,
    )
    if completed.returncode != 0:
        return 1
    try:
        envelope = json.loads(completed.stdout)
        status = envelope["data"]["status"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return 1
    return 0 if status in {"healthy", "degraded"} else 1


if __name__ == "__main__":
    sys.exit(main())
