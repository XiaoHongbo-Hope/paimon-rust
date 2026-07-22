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

import tempfile
import unittest
from pathlib import Path

from scripts.release_licenses import binary_notice, resolved_packages


class ResolvedPackagesTest(unittest.TestCase):
    def test_uses_normal_cargo_tree_packages(self) -> None:
        metadata = {
            "normal_packages": {("root", "1.0.0"), ("required", "2.0.0")},
            "packages": [
                {"name": "root", "version": "1.0.0"},
                {"name": "required", "version": "2.0.0"},
                {"name": "optional", "version": "3.0.0"},
            ],
        }
        self.assertEqual(
            [package["name"] for package in resolved_packages(metadata)],
            ["root", "required"],
        )

    def test_notice_uses_only_normal_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "NOTICE").write_text("Root notice\n", encoding="utf-8")
            packages = []
            for name, notice in (
                ("required", "Required dependency notice\n"),
                ("optional", "Optional dependency notice\n"),
            ):
                package_dir = root / name
                package_dir.mkdir()
                manifest = package_dir / "Cargo.toml"
                manifest.write_text("[package]\n", encoding="utf-8")
                (package_dir / "NOTICE").write_text(notice, encoding="utf-8")
                packages.append(
                    {
                        "name": name,
                        "version": "1.0.0",
                        "manifest_path": str(manifest),
                    }
                )

            metadata = {
                "normal_packages": {("required", "1.0.0")},
                "packages": packages,
            }
            notice = binary_notice(root, [metadata])
            self.assertIn("Required dependency notice", notice)
            self.assertNotIn("Optional dependency notice", notice)


if __name__ == "__main__":
    unittest.main()
