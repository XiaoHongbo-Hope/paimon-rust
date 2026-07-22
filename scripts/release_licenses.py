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

"""Generate artifact-exact legal metadata for native release artifacts."""

from __future__ import annotations

import argparse
import difflib
import html
import json
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


CARGO_ABOUT_VERSION = "0.9.1"
PYTHON_TARGETS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
)
GO_REPORTS = {
    "x86_64-unknown-linux-gnu": "THIRD-PARTY-LICENSES.linux.amd64.html",
    "aarch64-unknown-linux-gnu": "THIRD-PARTY-LICENSES.linux.arm64.html",
    "x86_64-apple-darwin": "THIRD-PARTY-LICENSES.darwin.amd64.html",
    "aarch64-apple-darwin": "THIRD-PARTY-LICENSES.darwin.arm64.html",
}


@dataclass(frozen=True)
class Report:
    component: str
    manifest: str
    target: str
    output: str


@dataclass(frozen=True)
class LicenseCorrection:
    crates: tuple[str, ...]
    license_crate: str
    license_path: str
    license_name: str
    anchor: str


@dataclass(frozen=True)
class BundledComponent:
    crate: str
    license_path: str
    component: str
    component_url: str
    license_name: str
    anchor: str
    components: tuple[str, ...] = ()
    targets: tuple[str, ...] = ()
    license_from_repository: bool = False
    required: bool = False
    relationship: str = "bundled by"
    crate_version: str = ""
    required_features: tuple[str, ...] = ()


@dataclass(frozen=True)
class NoticeComponent:
    crate: str
    notice_path: str
    components: tuple[str, ...]


ALLOC_CORRECTIONS = (
    LicenseCorrection(
        crates=("alloc-stdlib",),
        license_crate="alloc-no-stdlib",
        license_path="LICENSE",
        license_name="BSD 3-Clause License (rust-alloc-no-stdlib)",
        anchor="bsd-rust-alloc-no-stdlib",
    ),
    LicenseCorrection(
        crates=("aws-lc-sys",),
        license_crate="aws-lc-sys",
        license_path="LICENSE",
        license_name="AWS-LC License",
        anchor="aws-lc-license",
    ),
)

COMMON_MIT_CORRECTIONS = (
    LicenseCorrection(
        crates=("async-stream", "async-stream-impl"),
        license_crate="async-stream",
        license_path="LICENSE",
        license_name="MIT License (async-stream workspace)",
        anchor="mit-async-stream-workspace",
    ),
    LicenseCorrection(
        crates=("brotli-decompressor",),
        license_crate="brotli-decompressor",
        license_path="LICENSE",
        license_name="MIT License (brotli-decompressor)",
        anchor="mit-brotli-decompressor",
    ),
    LicenseCorrection(
        crates=("libm",),
        license_crate="libm",
        license_path="LICENSE.txt",
        license_name="MIT License (libm)",
        anchor="mit-libm",
    ),
)

PYTHON_MIT_CORRECTIONS = (
    LicenseCorrection(
        crates=(
            "ownedbytes",
            "tantivy-bitpacker",
            "tantivy-columnar",
            "tantivy-common",
            "tantivy-query-grammar",
            "tantivy-sstable",
            "tantivy-stacker",
            "tantivy-tokenizer-api",
        ),
        license_crate="tantivy",
        license_path="LICENSE",
        license_name="MIT License (Tantivy workspace)",
        anchor="mit-tantivy-workspace",
    ),
)

NOTICE_COMPONENTS = (
    NoticeComponent(
        crate="apache-avro",
        notice_path="NOTICE",
        components=("python", "go"),
    ),
    NoticeComponent(
        crate="arrow",
        notice_path="NOTICE.txt",
        components=("python", "go"),
    ),
    NoticeComponent(
        crate="datafusion",
        notice_path="NOTICE.txt",
        components=("python",),
    ),
    NoticeComponent(
        crate="object_store",
        notice_path="NOTICE.txt",
        components=("python", "go"),
    ),
)

BUNDLED_COMPONENTS = (
    BundledComponent(
        crate="zstd-sys",
        license_path="zstd/LICENSE",
        component="vendored Zstandard C sources",
        component_url="https://github.com/facebook/zstd",
        license_name="BSD 3-Clause License",
        anchor="bundled-zstandard-bsd-3-clause",
    ),
    BundledComponent(
        crate="rust-stemmers",
        license_path="algorithms/LICENSE",
        component="generated Snowball stemming algorithms",
        component_url="https://snowballstem.org/",
        license_name="BSD 3-Clause License",
        anchor="bundled-snowball-bsd-3-clause",
    ),
    BundledComponent(
        crate="regex-syntax",
        license_path="src/unicode_tables/LICENSE-UNICODE",
        component="generated Unicode character database tables",
        component_url="https://www.unicode.org/",
        license_name="Unicode Data Files and Software License",
        anchor="bundled-regex-syntax-unicode",
    ),
    BundledComponent(
        crate="liblzma-sys",
        license_path="xz/COPYING.0BSD",
        component="XZ Utils 5.8.3 liblzma C sources",
        component_url="https://github.com/tukaani-project/xz/tree/v5.8.3",
        license_name="BSD Zero Clause License",
        anchor="bundled-xz-utils-0bsd",
        components=("python",),
        required=True,
        relationship="statically linked through",
        crate_version="0.4.7",
        required_features=("static",),
    ),
    BundledComponent(
        crate="openssl-sys",
        license_path="third-party-licenses/openssl-1.1.1.LICENSE",
        component="OpenSSL 1.1.1k FIPS libssl and libcrypto shared libraries",
        component_url="https://github.com/openssl/openssl/tree/OpenSSL_1_1_1k",
        license_name="OpenSSL 1.1.1 and Original SSLeay Licenses",
        anchor="bundled-openssl-1.1.1",
        components=("python",),
        targets=("x86_64-unknown-linux-gnu",),
        license_from_repository=True,
        required=True,
        relationship="linked through",
    ),
    BundledComponent(
        crate="openssl-sys",
        license_path="third-party-licenses/openssl-1.1.1.LICENSE",
        component="OpenSSL 1.1.1w libssl and libcrypto shared libraries",
        component_url="https://github.com/openssl/openssl/tree/OpenSSL_1_1_1w",
        license_name="OpenSSL 1.1.1 and Original SSLeay Licenses",
        anchor="bundled-openssl-1.1.1",
        components=("python",),
        targets=("aarch64-unknown-linux-gnu",),
        license_from_repository=True,
        required=True,
        relationship="linked through",
    ),
)

ALLOC_PLACEHOLDER = "Copyright (c) &lt;year&gt; &lt;owner&gt;."
MIT_PLACEHOLDER = "Copyright (c) &lt;year&gt; &lt;copyright holders&gt;"
LICENSE_PLACEHOLDERS = (
    "&lt;year&gt;",
    "&lt;owner&gt;",
    "&lt;copyright holders&gt;",
    "<year>",
    "<owner>",
    "<copyright holders>",
)
ASF_DEVELOPED_NOTICE = re.compile(
    r"This product includes software developed at\n"
    r"The Apache Software Foundation \(https?://www\.apache\.org/\)\."
)


def repository_root() -> Path:
    return Path(
        subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
    )


def report_specs() -> list[Report]:
    reports = [
        Report(
            component="python",
            manifest="bindings/python/Cargo.toml",
            target=target,
            output=(f"bindings/python/licenses/{target}/THIRD-PARTY-LICENSES.html"),
        )
        for target in PYTHON_TARGETS
    ]
    reports.extend(
        Report(
            component="go",
            manifest="bindings/c/Cargo.toml",
            target=target,
            output=f"bindings/go/{filename}",
        )
        for target, filename in GO_REPORTS.items()
    )
    return reports


def verify_cargo_about(root: Path) -> None:
    output = subprocess.check_output(
        ["cargo", "about", "--version"], cwd=root, text=True
    ).strip()
    actual = output.rsplit(" ", 1)[-1]
    if actual != CARGO_ABOUT_VERSION:
        raise RuntimeError(
            f"cargo-about {CARGO_ABOUT_VERSION} is required, found {output!r}"
        )


def cargo_metadata(root: Path, report: Report) -> dict:
    output = subprocess.check_output(
        [
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--manifest-path",
            report.manifest,
            "--filter-platform",
            report.target,
        ],
        cwd=root,
        text=True,
    )
    return json.loads(output)


def generate_base_report(root: Path, report: Report, output: Path) -> str:
    subprocess.run(
        [
            "cargo",
            "about",
            "generate",
            "--frozen",
            "--fail",
            "--config",
            str(root / "about.toml"),
            "--manifest-path",
            report.manifest,
            "--target",
            report.target,
            "--output-file",
            str(output),
            str(root / "about.hbs"),
        ],
        cwd=root,
        check=True,
    )
    return output.read_text(encoding="utf-8")


def resolved_packages(metadata: dict) -> list[dict]:
    root_id = metadata["resolve"]["root"]
    if root_id is None:
        raise RuntimeError("cargo metadata did not identify a root package")
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    resolved = set()
    pending = [root_id]
    while pending:
        package_id = pending.pop()
        if package_id in resolved:
            continue
        resolved.add(package_id)
        pending.extend(
            dependency["pkg"]
            for dependency in nodes[package_id]["deps"]
            if any(kind["kind"] is None for kind in dependency["dep_kinds"])
        )
    return [package for package in metadata["packages"] if package["id"] in resolved]


def package_by_name(
    metadata: dict, crate_name: str, required: bool = True
) -> dict | None:
    matches = [
        package
        for package in resolved_packages(metadata)
        if package["name"] == crate_name
    ]
    if not matches and not required:
        return None
    if len(matches) != 1:
        versions = [package["version"] for package in matches]
        raise RuntimeError(
            f"expected exactly one resolved {crate_name} package, found {versions}"
        )
    return matches[0]


def package_features(metadata: dict, package: dict) -> set[str]:
    matches = [
        node["features"]
        for node in metadata["resolve"]["nodes"]
        if node["id"] == package["id"]
    ]
    if len(matches) != 1:
        raise RuntimeError(f"could not resolve features for {package['name']}")
    return set(matches[0])


def correction_html(metadata: dict, correction: LicenseCorrection) -> str:
    used_by = []
    for crate_name in correction.crates:
        package = package_by_name(metadata, crate_name)
        repository = package.get("repository") or (
            f"https://crates.io/crates/{crate_name}"
        )
        used_by.append(
            f'                    <li><a href="{html.escape(repository, quote=True)}">'
            f"{html.escape(crate_name)} {html.escape(package['version'])}</a></li>"
        )

    license_package = package_by_name(metadata, correction.license_crate)
    license_file = (
        Path(license_package["manifest_path"]).parent / correction.license_path
    )
    if not license_file.is_file():
        raise RuntimeError(f"corrected license file is missing: {license_file}")
    license_text = license_file.read_text(encoding="utf-8")

    return "\n".join(
        [
            '            <li class="license corrected-license">',
            f'                <h3 id="{html.escape(correction.anchor)}">'
            f"{html.escape(correction.license_name)}</h3>",
            "                <h4>Used by</h4>",
            '                <ul class="license-used-by">',
            *used_by,
            "                </ul>",
            f'                <pre class="license-text">{html.escape(license_text)}</pre>',
            "            </li>",
        ]
    )


def replace_placeholder_entry(
    report_text: str,
    metadata: dict,
    marker: str,
    corrections: tuple[LicenseCorrection, ...],
) -> str:
    if report_text.count(marker) != 1:
        raise RuntimeError(
            f"expected exactly one license placeholder {marker!r}, "
            f"found {report_text.count(marker)}"
        )
    marker_index = report_text.index(marker)
    entry_start = report_text.rfind('            <li class="license">', 0, marker_index)
    entry_end = report_text.find("            </li>", marker_index)
    if entry_start == -1 or entry_end == -1:
        raise RuntimeError(f"could not locate license entry for {marker!r}")
    entry_end += len("            </li>")
    placeholder_entry = report_text[entry_start:entry_end]

    actual_crates = set(
        re.findall(
            r'<li><a href="[^"]+">([A-Za-z0-9_-]+) [^<]+</a></li>',
            placeholder_entry,
        )
    )
    expected_crates = {
        crate_name for correction in corrections for crate_name in correction.crates
    }
    if actual_crates != expected_crates:
        raise RuntimeError(
            f"placeholder dependency set changed for {marker!r}: expected "
            f"{sorted(expected_crates)}, found {sorted(actual_crates)}"
        )

    replacement = "\n".join(
        correction_html(metadata, correction) for correction in corrections
    )
    return report_text[:entry_start] + replacement + report_text[entry_end:]


def bundled_component_html(
    root: Path, report: Report, report_text: str, metadata: dict
) -> str:
    items = []
    for component in BUNDLED_COMPONENTS:
        if component.components and report.component not in component.components:
            continue
        if component.targets and report.target not in component.targets:
            continue
        package = package_by_name(metadata, component.crate, required=False)
        if package is None:
            if component.required:
                raise RuntimeError(
                    f"required bundled component crate is missing: {component.crate}"
                )
            continue
        if component.crate_version and package["version"] != component.crate_version:
            raise RuntimeError(
                f"expected {component.crate} {component.crate_version}, "
                f"found {package['version']}"
            )
        missing_features = set(component.required_features) - package_features(
            metadata, package
        )
        if missing_features:
            raise RuntimeError(
                f"{component.crate} is missing required features: "
                f"{sorted(missing_features)}"
            )
        marker = f">{component.crate} {package['version']}</a>"
        if marker not in report_text:
            if component.required:
                raise RuntimeError(
                    f"required bundled component is absent from report: {component.crate}"
                )
            # Cargo metadata may include workspace-unified features.
            continue
        if component.license_from_repository:
            license_file = root / component.license_path
        else:
            license_file = (
                Path(package["manifest_path"]).parent / component.license_path
            )
        if not license_file.is_file():
            raise RuntimeError(f"bundled license file is missing: {license_file}")
        license_text = license_file.read_text(encoding="utf-8")
        repository = package.get("repository") or (
            f"https://crates.io/crates/{component.crate}"
        )
        items.append(
            "\n".join(
                [
                    '            <li class="license bundled-subcomponent">',
                    f'                <h3 id="{html.escape(component.anchor)}">'
                    f"{html.escape(component.license_name)}</h3>",
                    "                <h4>Bundled component</h4>",
                    '                <ul class="license-used-by">',
                    "                    <li>",
                    f'                        <a href="{html.escape(component.component_url, quote=True)}">'
                    f"{html.escape(component.component)}</a>, "
                    f"{html.escape(component.relationship)}",
                    f'                        <a href="{html.escape(repository, quote=True)}">'
                    f"{html.escape(component.crate)} {html.escape(package['version'])}</a>",
                    "                    </li>",
                    "                </ul>",
                    f'                <pre class="license-text">{html.escape(license_text)}</pre>',
                    "            </li>",
                ]
            )
        )

    if not items:
        return ""
    return "\n".join(
        [
            "",
            "        <h2>Licenses for bundled native and source components</h2>",
            "        <p>",
            "            These components ship with the native artifact but have",
            "            license files outside their Rust package metadata.",
            "        </p>",
            '        <ul class="licenses-list bundled-subcomponents">',
            *items,
            "        </ul>",
        ]
    )


def complete_report(
    root: Path, base_report: str, report: Report, metadata: dict
) -> str:
    result = replace_placeholder_entry(
        base_report, metadata, ALLOC_PLACEHOLDER, ALLOC_CORRECTIONS
    )
    mit_corrections = COMMON_MIT_CORRECTIONS
    if report.component == "python":
        mit_corrections += PYTHON_MIT_CORRECTIONS
    result = replace_placeholder_entry(
        result, metadata, MIT_PLACEHOLDER, mit_corrections
    )

    for placeholder in LICENSE_PLACEHOLDERS:
        if placeholder in result:
            raise RuntimeError(
                f"generated report still contains license placeholder {placeholder!r}"
            )

    description = (
        "\n        <p><strong>Artifact:</strong> "
        f"<code>{html.escape(report.component)}</code></p>"
        "\n        <p><strong>Rust target:</strong> "
        f"<code>{html.escape(report.target)}</code></p>"
        "\n        <p><strong>Root manifest:</strong> "
        f"<code>{html.escape(report.manifest)}</code></p>"
    )
    first_paragraph_end = result.find("</p>")
    if first_paragraph_end == -1:
        raise RuntimeError("about.hbs output has no introductory paragraph")
    first_paragraph_end += len("</p>")
    result = result[:first_paragraph_end] + description + result[first_paragraph_end:]

    closing_main = result.rfind("    </main>")
    if closing_main == -1:
        raise RuntimeError("about.hbs output has no closing main element")
    result = (
        result[:closing_main]
        + bundled_component_html(root, report, result, metadata)
        + "\n"
        + result[closing_main:]
    )
    return "\n".join(line.rstrip() for line in result.rstrip().splitlines()) + "\n"


def binary_license(apache_license: str, heading: str, reports: list[str]) -> str:
    appendix = [
        "",
        "=" * 79,
        "BUNDLED THIRD-PARTY COMPONENTS",
        "=" * 79,
        "",
        heading,
        "The component inventory, copyright notices, and complete license texts",
        "are provided in:",
        "",
    ]
    appendix.extend(f"    {report}" for report in reports)
    return apache_license.rstrip() + "\n" + "\n".join(appendix) + "\n"


def notice_paragraphs(text: str) -> list[str]:
    normalized = "\n".join(line.rstrip() for line in text.strip().splitlines())
    return [part.strip() for part in re.split(r"\n\s*\n", normalized) if part.strip()]


def binary_notice(root: Path, component: str, metadata_by_target: list[dict]) -> str:
    paragraphs = notice_paragraphs((root / "NOTICE").read_text(encoding="utf-8"))
    seen_paragraphs = set(paragraphs)
    seen_notices = set()

    for notice_component in NOTICE_COMPONENTS:
        if component not in notice_component.components:
            continue
        found = False
        for metadata in metadata_by_target:
            package = package_by_name(metadata, notice_component.crate, required=False)
            if package is None:
                continue
            found = True
            notice_file = (
                Path(package["manifest_path"]).parent / notice_component.notice_path
            )
            if not notice_file.is_file():
                raise RuntimeError(f"dependency notice is missing: {notice_file}")
            notice_text = notice_file.read_text(encoding="utf-8")
            normalized_notice = "\n\n".join(notice_paragraphs(notice_text))
            if normalized_notice in seen_notices:
                continue
            seen_notices.add(normalized_notice)
            for paragraph in notice_paragraphs(notice_text):
                if ASF_DEVELOPED_NOTICE.fullmatch(paragraph):
                    continue
                if paragraph in seen_paragraphs:
                    continue
                seen_paragraphs.add(paragraph)
                paragraphs.append(paragraph)
        if not found:
            raise RuntimeError(
                f"required NOTICE dependency is missing: {notice_component.crate}"
            )

    return "\n\n".join(paragraphs) + "\n"


def generated_files(root: Path) -> dict[Path, str]:
    verify_cargo_about(root)
    for name in ("LICENSE", "NOTICE"):
        source = root / name
        copy = root / "bindings/python" / name
        if source.read_bytes() != copy.read_bytes():
            raise RuntimeError(f"restore bindings/python/{name} before generation")
    result = {}
    metadata_by_report = {}
    with tempfile.TemporaryDirectory(prefix="paimon-rust-license-reports-") as temp:
        temp_root = Path(temp)
        for index, report in enumerate(report_specs()):
            print(f"generating {report.component} license report for {report.target}")
            base = generate_base_report(
                root, report, temp_root / f"report-{index}.html"
            )
            metadata = cargo_metadata(root, report)
            metadata_by_report[(report.component, report.target)] = metadata
            result[root / report.output] = complete_report(root, base, report, metadata)

    apache_license = (root / "LICENSE").read_text(encoding="utf-8")
    for target in PYTHON_TARGETS:
        license_dir = root / "bindings/python/licenses" / target
        result[license_dir / "LICENSE"] = binary_license(
            apache_license,
            f"This binary wheel bundles the Rust native library for {target}.",
            ["THIRD-PARTY-LICENSES.html"],
        )
        result[license_dir / "NOTICE"] = binary_notice(
            root,
            "python",
            [metadata_by_report[("python", target)]],
        )

    go_reports = list(GO_REPORTS.values())
    result[root / "bindings/go/LICENSE"] = binary_license(
        apache_license,
        "This Go module bundles Rust native libraries for four release targets.",
        go_reports,
    )
    result[root / "bindings/go/NOTICE"] = binary_notice(
        root,
        "go",
        [metadata_by_report[("go", target)] for target in GO_REPORTS],
    )
    return result


def check_files(files: dict[Path, str], root: Path) -> int:
    failed = False
    for path, expected in files.items():
        if not path.is_file():
            print(f"missing generated license file: {path.relative_to(root)}")
            failed = True
            continue
        expected_bytes = expected.encode("utf-8")
        actual_bytes = path.read_bytes()
        if actual_bytes == expected_bytes:
            continue
        failed = True
        print(f"stale generated license file: {path.relative_to(root)}")
        actual = actual_bytes.decode("utf-8")
        diff = difflib.unified_diff(
            actual.splitlines(),
            expected.splitlines(),
            fromfile=str(path.relative_to(root)),
            tofile=f"generated/{path.relative_to(root)}",
            lineterm="",
        )
        for line in list(diff)[:200]:
            print(line)
    return 1 if failed else 0


def write_files(files: dict[Path, str], root: Path) -> None:
    for path, content in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode("utf-8"))
        print(f"generated {path.relative_to(root)}")


def stage_python(root: Path, target: str) -> None:
    if target not in PYTHON_TARGETS:
        raise ValueError(f"unsupported Python release target: {target}")
    source_dir = root / "bindings/python/licenses" / target
    destination_dir = root / "bindings/python"
    for name in ("LICENSE", "NOTICE", "THIRD-PARTY-LICENSES.html"):
        source = source_dir / name
        if not source.is_file():
            raise FileNotFoundError(source)
        (destination_dir / name).write_bytes(source.read_bytes())
        print(f"staged {source.relative_to(root)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--check",
        action="store_true",
        help="fail if committed legal files differ from reproducible output",
    )
    group.add_argument(
        "--stage-python",
        metavar="TARGET",
        help="stage one target's legal files for a maturin wheel build",
    )
    args = parser.parse_args()
    root = repository_root()

    try:
        if args.stage_python:
            stage_python(root, args.stage_python)
            return 0
        files = generated_files(root)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"release license generation failed: {error}", file=sys.stderr)
        return 1

    if args.check:
        return check_files(files, root)
    write_files(files, root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
