#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Release tooling: requires Python 3.11+ (constants.py uses tomllib).

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "This script requires Python 3.11 or newer (uses tomllib). "
        f"Current: {sys.version}. Use python3.11+ or see docs for release requirements."
    )

import difflib
import subprocess
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from functools import cache

from constants import PACKAGES, ROOT_DIR

CARGO_DENY_VERSION = "0.19.6"


@cache
def verify_cargo_deny() -> None:
    output = subprocess.check_output(
        ["cargo", "deny", "--version"], cwd=ROOT_DIR, text=True
    ).strip()
    actual = output.rsplit(" ", 1)[-1]
    if actual != CARGO_DENY_VERSION:
        raise RuntimeError(
            f"cargo-deny {CARGO_DENY_VERSION} is required, found {output!r}"
        )


def cargo_deny(spec, *args, capture_output=False):
    verify_cargo_deny()
    return subprocess.run(
        [
            "cargo",
            "deny",
            "--locked",
            "--all-features",
            "--manifest-path",
            str(ROOT_DIR / spec.manifest_path),
            *args,
        ],
        cwd=ROOT_DIR,
        check=not capture_output,
        capture_output=capture_output,
        text=True,
    )


def check_single_package(spec):
    print(f"Checking dependencies of {spec.output_dir}")
    cargo_deny(spec, "check", "licenses")


def check_deps():
    checked = set()
    for spec in PACKAGES:
        if spec.manifest_path in checked:
            continue
        checked.add(spec.manifest_path)
        check_single_package(spec)


def dependency_report(spec):
    result = cargo_deny(spec, "list", "-f", "tsv", "-t", "0.6", capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"cargo deny list failed for {spec.manifest_path}: "
            f"{result.stderr or result.stdout}"
        )
    return result.stdout


def output_file(spec):
    output_dir = ROOT_DIR if spec.output_dir == "." else ROOT_DIR / spec.output_dir
    return output_dir / "DEPENDENCIES.rust.tsv"


def generate_single_package(spec):
    print(f"Generating dependencies {spec.output_dir}")
    out_file = output_file(spec)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_bytes(dependency_report(spec).encode("utf-8"))


def generate_deps():
    for spec in PACKAGES:
        generate_single_package(spec)


def verify_deps():
    failed = False
    for spec in PACKAGES:
        expected = dependency_report(spec)
        out_file = output_file(spec)
        if not out_file.exists():
            print(f"Missing dependency report: {out_file.relative_to(ROOT_DIR)}")
            failed = True
            continue
        expected_bytes = expected.encode("utf-8")
        actual_bytes = out_file.read_bytes()
        if actual_bytes == expected_bytes:
            continue
        failed = True
        print(f"Stale dependency report: {out_file.relative_to(ROOT_DIR)}")
        actual = actual_bytes.decode("utf-8")
        diff = difflib.unified_diff(
            actual.splitlines(),
            expected.splitlines(),
            fromfile=str(out_file.relative_to(ROOT_DIR)),
            tofile=f"generated/{out_file.relative_to(ROOT_DIR)}",
            lineterm="",
        )
        for line in list(diff)[:200]:
            print(line)
    if failed:
        raise RuntimeError("dependency reports are missing or stale")


if __name__ == "__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.set_defaults(func=parser.print_help)
    subparsers = parser.add_subparsers()

    parser_check = subparsers.add_parser(
        "check", description="Check dependencies", help="Check dependencies"
    )
    parser_check.set_defaults(func=check_deps)

    parser_generate = subparsers.add_parser(
        "generate", description="Generate dependencies", help="Generate dependencies"
    )
    parser_generate.set_defaults(func=generate_deps)

    parser_verify = subparsers.add_parser(
        "verify",
        description="Verify committed dependency reports",
        help="Verify committed dependency reports",
    )
    parser_verify.set_defaults(func=verify_deps)

    args = parser.parse_args()
    arg_dict = dict(vars(args))
    del arg_dict["func"]
    args.func(**arg_dict)
