PyPaimon native-plan SDK bundle
================================

Install with CPython 3.10 or later:

    python install.py

The installer selects the compatible pypaimon-rust wheel automatically and
uses the configured Python package index for third-party dependencies.

Included platforms:

* Linux x86_64 (manylinux2014)
* Linux aarch64 (manylinux_2_28)
* macOS x86_64
* macOS Apple Silicon
* Windows x86_64

Enable Rust scan planning with the PyPaimon table option:

    scan.native-plan.enabled=true

Only scan planning uses Rust. Data reads and writes continue through PyPaimon.

Sources:

* Apache Paimon master 30bfd4a600a930079b15f5e6bacea34b8d3ff4c8
* Apache Paimon Rust main d66d3223159cd72bead6c36cc2531fec8ab35f7b
