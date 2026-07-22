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

import tarfile
import unittest

from scripts.verify_python_wheels import (
    VerificationError,
    index_tar_members,
    normalized_relative_path,
)


class ArchivePathTest(unittest.TestCase):
    def test_rejects_non_canonical_paths(self) -> None:
        for value in ("root/./LICENSE", "root//LICENSE", "C:/LICENSE"):
            with self.subTest(value=value):
                with self.assertRaises(VerificationError):
                    normalized_relative_path(value, "archive member")

    def test_rejects_tar_links(self) -> None:
        member = tarfile.TarInfo("root/LICENSE")
        member.type = tarfile.SYMTYPE
        member.linkname = "NOTICE"
        with self.assertRaises(VerificationError):
            index_tar_members([member], "sdist")

    def test_rejects_duplicate_tar_members(self) -> None:
        first = tarfile.TarInfo("root/LICENSE")
        second = tarfile.TarInfo("root/LICENSE")
        with self.assertRaises(VerificationError):
            index_tar_members([first, second], "sdist")

    def test_rejects_portable_tar_collisions(self) -> None:
        upper = tarfile.TarInfo("root/LICENSE")
        lower = tarfile.TarInfo("root/license")
        with self.assertRaises(VerificationError):
            index_tar_members([upper, lower], "sdist")

    def test_indexes_regular_tar_members(self) -> None:
        license_member = tarfile.TarInfo("root/LICENSE")
        notice_member = tarfile.TarInfo("root/NOTICE")
        indexed = index_tar_members([license_member, notice_member], "sdist")
        self.assertEqual(set(indexed), {"root/LICENSE", "root/NOTICE"})


if __name__ == "__main__":
    unittest.main()
