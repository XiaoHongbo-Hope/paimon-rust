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

"""Verify Go native release artifacts and their target-specific licenses."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Artifact:
    library: str
    report: str
    format_markers: tuple[str, ...]
    architecture_markers: tuple[str, ...]


ARTIFACTS = {
    "x86_64-unknown-linux-gnu": Artifact(
        library="libpaimon_c.linux.amd64.so.zst",
        report="THIRD-PARTY-LICENSES.linux.amd64.html",
        format_markers=("elf 64-bit", "shared object"),
        architecture_markers=("x86-64", "x86_64"),
    ),
    "aarch64-unknown-linux-gnu": Artifact(
        library="libpaimon_c.linux.arm64.so.zst",
        report="THIRD-PARTY-LICENSES.linux.arm64.html",
        format_markers=("elf 64-bit", "shared object"),
        architecture_markers=("aarch64", "arm64"),
    ),
    "x86_64-apple-darwin": Artifact(
        library="libpaimon_c.darwin.amd64.dylib.zst",
        report="THIRD-PARTY-LICENSES.darwin.amd64.html",
        format_markers=("mach-o 64-bit", "dynamically linked shared library"),
        architecture_markers=("x86_64", "x86-64"),
    ),
    "aarch64-apple-darwin": Artifact(
        library="libpaimon_c.darwin.arm64.dylib.zst",
        report="THIRD-PARTY-LICENSES.darwin.arm64.html",
        format_markers=("mach-o 64-bit", "dynamically linked shared library"),
        architecture_markers=("arm64", "aarch64"),
    ),
}

LICENSE_PLACEHOLDERS = (
    "&lt;year&gt;",
    "&lt;owner&gt;",
    "&lt;copyright holders&gt;",
    "<year>",
    "<owner>",
    "<copyright holders>",
)


def repository_root() -> Path:
    return Path(__file__).resolve().parent.parent


def require_file(path: Path) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"missing or empty release file: {path}")


def require_equal(actual: Path, expected: Path) -> None:
    require_file(actual)
    require_file(expected)
    if actual.read_bytes() != expected.read_bytes():
        raise ValueError(f"{actual} does not match {expected}")


def verify_report(report: Path, expected: Path, target: str) -> None:
    require_equal(report, expected)
    report_text = report.read_text(encoding="utf-8")
    required_markers = (
        "<strong>Artifact:</strong> <code>go</code>",
        f"<strong>Rust target:</strong> <code>{target}</code>",
        "<strong>Root manifest:</strong> <code>bindings/c/Cargo.toml</code>",
    )
    for marker in required_markers:
        if marker not in report_text:
            raise ValueError(f"{report} is missing {marker!r}")
    for placeholder in LICENSE_PLACEHOLDERS:
        if placeholder in report_text:
            raise ValueError(f"{report} contains placeholder {placeholder!r}")


def verify_library(library: Path, artifact: Artifact) -> None:
    require_file(library)
    with tempfile.TemporaryDirectory(prefix="paimon-go-release-") as temp_dir:
        unpacked = Path(temp_dir) / library.name.removesuffix(".zst")
        with unpacked.open("wb") as destination:
            result = subprocess.run(
                ["zstd", "-d", "-q", "-c", str(library)],
                stdout=destination,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
        if result.returncode != 0:
            raise ValueError(f"cannot decompress {library}: {result.stderr.strip()}")
        require_file(unpacked)
        description = subprocess.check_output(
            ["file", "-b", str(unpacked)], text=True
        ).strip()

    normalized = description.lower()
    if any(marker not in normalized for marker in artifact.format_markers):
        raise ValueError(f"unexpected binary format for {library}: {description}")
    if not any(marker in normalized for marker in artifact.architecture_markers):
        raise ValueError(f"unexpected binary architecture for {library}: {description}")


def verify_target(root: Path, artifacts_dir: Path, target: str) -> None:
    artifact = ARTIFACTS[target]
    library = artifacts_dir / artifact.library
    report = artifacts_dir / artifact.report
    expected_report = root / "bindings/go" / artifact.report
    verify_library(library, artifact)
    verify_report(report, expected_report, target)
    print(f"verified {artifact.library}: {target}")


def verify_release_legal_files(root: Path, artifacts_dir: Path) -> None:
    for name in ("LICENSE", "NOTICE"):
        require_equal(artifacts_dir / name, root / "bindings/go" / name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifacts-dir",
        required=True,
        type=Path,
        help="directory containing downloaded or locally built release assets",
    )
    parser.add_argument(
        "--target",
        choices=tuple(ARTIFACTS),
        help="verify one matrix target; omit to verify the complete release set",
    )
    args = parser.parse_args()

    root = repository_root()
    artifacts_dir = args.artifacts_dir.resolve()
    try:
        if args.target:
            verify_target(root, artifacts_dir, args.target)
        else:
            verify_release_legal_files(root, artifacts_dir)
            for target in ARTIFACTS:
                verify_target(root, artifacts_dir, target)
    except (OSError, UnicodeError, ValueError, subprocess.SubprocessError) as error:
        print(f"Go release artifact verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
