#!/usr/bin/env python3

import argparse
import hashlib
import pathlib
import shutil
import zipfile


PACKAGE_NAME = "pypaimon-native-plan-2.1.dev20260809-rust20260810"
EXPECTED_RUST_WHEELS = (
    "manylinux2014_x86_64",
    "manylinux_2_28_aarch64",
    "macosx_10_12_x86_64",
    "macosx_11_0_arm64",
    "win_amd64",
)


def copy_files(files, destination):
    for source in files:
        shutil.copy2(source, destination / source.name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust-artifacts", type=pathlib.Path, required=True)
    parser.add_argument("--pypaimon-dist", type=pathlib.Path, required=True)
    parser.add_argument("--templates", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()

    package = args.output / PACKAGE_NAME
    wheels = package / "wheels"
    wheels.mkdir(parents=True)

    rust_wheels = sorted(args.rust_artifacts.rglob("pypaimon_rust-*.whl"))
    names = [wheel.name for wheel in rust_wheels]
    missing = [tag for tag in EXPECTED_RUST_WHEELS
               if not any(tag in name for name in names)]
    if missing:
        raise RuntimeError("Missing Rust wheels: %s" % ", ".join(missing))
    if len(rust_wheels) != len(EXPECTED_RUST_WHEELS):
        raise RuntimeError("Expected 5 Rust wheels, found %d" % len(rust_wheels))

    pypaimon_wheels = sorted(args.pypaimon_dist.glob("pypaimon-*.whl"))
    if len(pypaimon_wheels) != 1:
        raise RuntimeError("Expected 1 PyPaimon wheel, found %d" % len(pypaimon_wheels))

    copy_files(rust_wheels, wheels)
    copy_files(pypaimon_wheels, wheels)
    copy_files(sorted(args.rust_artifacts.rglob("pypaimon_rust-*.tar.gz")), wheels)
    copy_files(sorted(args.pypaimon_dist.glob("pypaimon-*.tar.gz")), wheels)
    shutil.copy2(args.templates / "install.py", package / "install.py")
    shutil.copy2(args.templates / "README.txt", package / "README.txt")

    archive = args.output / (PACKAGE_NAME + ".zip")
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as output:
        for path in sorted(package.rglob("*")):
            if path.is_file():
                output.write(path, path.relative_to(args.output))

    digest = hashlib.sha512(archive.read_bytes()).hexdigest()
    archive.with_suffix(".zip.sha512").write_text(
        "%s  %s\n" % (digest, archive.name), encoding="utf-8")
    print(archive)


if __name__ == "__main__":
    main()
