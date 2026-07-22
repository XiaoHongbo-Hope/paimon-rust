<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Bundled third-party licenses

`openssl-1.1.1.LICENSE` covers OpenSSL 1.1.1k FIPS on x86_64 Linux and
OpenSSL 1.1.1w on aarch64 Linux. Both versions use the same license text.

The XZ Utils 5.8.3 license is read from the locked `liblzma-sys` crate at
`xz/COPYING.0BSD`.

Target-specific Python and Go reports are generated in their binary build jobs.
They are intentionally not committed or included in the ASF source archive.

Sources:

- <https://github.com/openssl/openssl/blob/OpenSSL_1_1_1k/LICENSE>
- <https://github.com/openssl/openssl/blob/OpenSSL_1_1_1w/LICENSE>
