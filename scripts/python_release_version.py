#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Set a unique Python version for release-candidate artifacts."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path


TAG_PATTERN = re.compile(r"^v(?P<base>\d+\.\d+\.\d+)(?:-rc(?P<rc>[1-9]\d*))?$")
DYNAMIC_VERSION = 'dynamic = ["version"]'


def workspace_version(root: Path) -> str:
    with (root / "Cargo.toml").open("rb") as source:
        return str(tomllib.load(source)["workspace"]["package"]["version"])


def python_version(tag: str, expected_base: str) -> str:
    match = TAG_PATTERN.fullmatch(tag)
    if match is None:
        raise ValueError(f"invalid release tag: {tag}")
    base = match.group("base")
    if base != expected_base:
        raise ValueError(f"tag {base} does not match workspace {expected_base}")
    rc = match.group("rc")
    return base if rc is None else f"{base}rc{rc}"


def update_pyproject(path: Path, version: str, expected_base: str) -> None:
    if version == expected_base:
        return
    content = path.read_text(encoding="utf-8")
    if content.count(DYNAMIC_VERSION) != 1:
        raise ValueError(f"unexpected dynamic version setting in {path}")
    path.write_text(
        content.replace(DYNAMIC_VERSION, f'version = "{version}"', 1),
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--pyproject", type=Path)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    try:
        base = workspace_version(root)
        version = python_version(args.tag, base)
        if args.pyproject is not None:
            update_pyproject(root / args.pyproject, version, base)
    except (KeyError, OSError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"Python release version failed: {error}", file=sys.stderr)
        return 1

    print(version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
