#!/usr/bin/env python3

import pathlib
import subprocess
import sys


WHEELS = pathlib.Path(__file__).resolve().parent / "wheels"


def main():
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--find-links",
        str(WHEELS),
        "pypaimon==2.1.dev20260809",
        "pypaimon-rust==0.4.0",
    ]
    command.extend(sys.argv[1:])
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
