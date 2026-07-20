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

import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class PackageSpec:
    output_dir: str
    manifest_path: str


def list_packages() -> list[PackageSpec]:
    """List dependency report inputs and destinations."""
    root_cargo = ROOT_DIR / "Cargo.toml"
    if not root_cargo.exists():
        return [PackageSpec(".", "Cargo.toml")]
    with open(root_cargo, "rb") as f:
        data = tomllib.load(f)
    members = data.get("workspace", {}).get("members", [])
    if not isinstance(members, list):
        return [PackageSpec(".", "Cargo.toml")]
    packages = [PackageSpec(".", "Cargo.toml")]
    for m in members:
        if isinstance(m, str) and m:
            packages.append(PackageSpec(m, f"{m}/Cargo.toml"))
    # The Go module embeds paimon-c.
    packages.append(PackageSpec("bindings/go", "bindings/c/Cargo.toml"))
    return packages


PACKAGES = list_packages()
