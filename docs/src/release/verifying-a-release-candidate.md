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

# Verifying a Release Candidate

This document describes how to verify a release candidate (RC) of **Apache Paimon Rust** (Rust crates, Python binding, Go binding) from the [paimon-rust](https://github.com/apache/paimon-rust) repository. It is intended for anyone participating in the release vote (binding or non-binding) and is based on [ASF Release Policy](https://www.apache.org/legal/release-policy.html), adapted for the paimon-rust source distribution and tooling.

## Validating Distributions

The release vote email includes links to:

- **Distribution archive:** source tarball (`paimon-rust-${RELEASE_VERSION}.tar.gz`) on [dist.apache.org dev](https://dist.apache.org/repos/dist/dev/paimon/)
- **Signature file:** `paimon-rust-${RELEASE_VERSION}.tar.gz.asc`
- **Checksum file:** `paimon-rust-${RELEASE_VERSION}.tar.gz.sha512`
- **KEYS file:** [https://downloads.apache.org/paimon/KEYS](https://downloads.apache.org/paimon/KEYS)

Download the archive (`.tar.gz`), `.asc`, and `.sha512` from the RC directory (e.g. `paimon-rust-${RELEASE_VERSION}-rc${RC_NUM}/`) and the KEYS file. Then follow the steps below to verify signatures and checksums.

## Verifying Signatures

First, import the keys into your local keyring:

```bash
curl https://downloads.apache.org/paimon/KEYS -o KEYS
gpg --import KEYS
```

Next, verify the `.asc` file:

```bash
gpg --verify paimon-rust-${RELEASE_VERSION}.tar.gz.asc paimon-rust-${RELEASE_VERSION}.tar.gz
```

If verification succeeds, you will see a message like:

```text
gpg: Signature made ...
gpg: using RSA key ...
gpg: Good signature from "Release Manager Name (CODE SIGNING KEY) <...@apache.org>"
```

## Verifying Checksums

Verify the tarball using the provided `.sha512` file. The `.sha512` file lists the expected SHA-512 hash for the corresponding archive; `-c` reads that file and checks the archive.

**On macOS (shasum):**

```bash
shasum -a 512 -c paimon-rust-${RELEASE_VERSION}.tar.gz.sha512
```

**On Linux (sha512sum):**

```bash
sha512sum -c paimon-rust-${RELEASE_VERSION}.tar.gz.sha512
```

If the verification is successful, you will see a message like:

```text
paimon-rust-${RELEASE_VERSION}.tar.gz: OK
```

## Verifying Build

Extract the source release archive and verify that it builds (and optionally that tests pass). You need **Rust** (see [rust-toolchain.toml](https://github.com/apache/paimon-rust/blob/main/rust-toolchain.toml) for the expected version).

```bash
tar -xzf paimon-rust-${RELEASE_VERSION}.tar.gz
cd paimon-rust-${RELEASE_VERSION}
```

Build the workspace:

```bash
cargo build --locked --workspace --release
```

For Python binding, see `bindings/python/`. For Go binding, see `bindings/go/`.

## Verifying LICENSE and NOTICE

Unzip the source release archive and verify that:

1. The root **LICENSE**, **NOTICE**, and **Cargo.lock** files are present and correct.
2. All committed `DEPENDENCIES.rust.tsv` reports match the locked dependency graph.
3. All files that need it have ASF license headers.
4. All dependencies have been checked for license compatibility with the [ASF third-party license policy](http://www.apache.org/legal/resolved.html#category-x).
5. Compatible non-ASL 2.0 licenses and bundled source components are documented in the generated dependency or third-party license reports.

The project uses [cargo-deny](https://embarkstudios.github.io/cargo-deny/) and [cargo-about](https://github.com/EmbarkStudios/cargo-about). Reproduce the committed reports with:

```bash
cargo install cargo-deny --version 0.19.6 --locked
cargo install cargo-about --version 0.9.1 --locked
cargo fetch --locked
python3 scripts/dependencies.py check
python3 scripts/dependencies.py verify
python3 scripts/release_licenses.py --check
```

Also inspect the publishable Rust package inventories:

```bash
cargo package --locked -p paimon --list
cargo package --locked -p paimon-datafusion --list
```

Each package must contain its crate-local `LICENSE`, `NOTICE`, `DEPENDENCIES.rust.tsv`, shared test helpers, and `testdata/` fixtures. Python wheels must contain `LICENSE`, `NOTICE`, and `THIRD-PARTY-LICENSES.html`; the Go release must contain `LICENSE`, `NOTICE`, four native libraries, and the four matching third-party license reports.

## Testing Features

For any user-facing feature included in a release, we aim to ensure it is functional, usable, and well-documented. Release managers may create testing issues that outline key scenarios to validate; these are open to all community members.

**Per-component verification:**

- **Rust crates:** You can depend on the RC via its git tag (e.g. in your `Cargo.toml`: `paimon = { git = "https://github.com/apache/paimon-rust", tag = "v${RELEASE_VERSION}-rc${RC_NUM}" }`) and build your own test project to verify. Alternatively, build from the source release; see [Getting Started](https://paimon.apache.org/docs/rust/getting-started/) for usage examples.
- **Python binding:** The RC is published to **TestPyPI**; install the client from TestPyPI and write your own test cases to verify:

    ```bash
    pip install -i https://test.pypi.org/simple/ pypaimon-rust==${RELEASE_VERSION}
    ```

- **Go binding:** The RC is published as a Go module tag `bindings/go/v${RELEASE_VERSION}-rc${RC_NUM}`; see [Go Binding](https://paimon.apache.org/docs/rust/go-binding/) for usage. Add it to your Go project and write test cases to verify:

    ```bash
    go get github.com/apache/paimon-rust/bindings/go@v${RELEASE_VERSION}-rc${RC_NUM}
    ```

## Voting

Votes are cast by replying to the vote email on the dev mailing list with **+1**, **0**, or **-1**.

In addition to your vote, it is customary to state whether your vote is **binding** or **non-binding**. Only members of the PMC have formally binding votes. If unsure, you can state that your vote is non-binding. See [Apache Foundation Voting](https://www.apache.org/foundation/voting.html).

It is recommended to include a short list of what you verified (e.g. signatures, checksums, build, tests, LICENSE/NOTICE). This helps the community see what has been checked and what might still be missing.

**Checklist you can reference in your vote:**

- [ ] [Validating distributions](#validating-distributions)
- [ ] [Verifying signatures](#verifying-signatures)
- [ ] [Verifying checksums](#verifying-checksums)
- [ ] [Verifying build](#verifying-build)
- [ ] [Verifying LICENSE and NOTICE](#verifying-license-and-notice)
- [ ] [Testing features](#testing-features)
