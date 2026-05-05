#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. A copy of the License
# is located at http://www.apache.org/licenses/LICENSE-2.0
#
# Copyright OpenSearch Contributors. SPDX-License-Identifier: Apache-2.0
"""
adapt_iq.py — Transform an upstream Apache Calcite `.iq` test file into the
adapted form consumed by `AnsiSqlQuidemIT` in this repo.

Background
----------
`integ-test/src/test/resources/ansi/*.iq` holds 15 Quidem files that were
hand-adapted from Calcite's own `core/src/test/resources/sql/*.iq` fixtures.
Rather than re-doing that work every time Calcite ships a new `.iq`, this
script mechanises the systematic changes so contributors can pull new files
with a single invocation:

    ./scripts/calcite-iq/adapt_iq.py \
        path/to/calcite/core/src/test/resources/sql/foo.iq \
        -o integ-test/src/test/resources/ansi/foo.iq

Transformations applied (derived from diffing the 15 adapted files against
Calcite upstream — see docs/mustang-followup-ansi-sql-it.md)
---------------------------------------------------------------------------
  P1. All `!use <schema>` lines are normalised to `!use ansi`. Calcite uses
      many schema names (`scott`, `post`, `catchall`, `foodmart`, ...);
      `OpenSearchConnectionFactory` exposes them all as aliases of the same
      OpenSearch JDBC endpoint, so the adapted files pick a single canonical
      name.
  P2. `!plan` directives and the query-plan block that follows each of them
      are stripped entirely. The plan block runs from the `!plan` line up
      to (but not including) the next Quidem directive (`!ok`, `!error`,
      `!if`, `!set`, `!use`, `!type`, `!verify`, `!skip`, `!plan`).
      `!plan` directives embedded inside `!if (...) { ... }` blocks are
      also stripped.
  P3. Java class prefixes in `!error` payloads are shortened:
          java.lang. -> lang.
          java.sql.  -> sql.
      Other `java.*` prefixes are left alone.
  P4. If the file does not already start with our standard header
      (ASF license + `!use ansi` + `!set outputformat mysql`), inject it.
      Any `!use` / `!set outputformat` lines already present in the file
      are left in place so we don't double up.
  P5. If the file does not end with `# End <basename>`, append that trailer.
  P6. `!verify` directives are preserved. The adapted files keep the one
      occurrence from `sort.iq`; reference connections in
      `OpenSearchConnectionFactory` return `null`, so `!verify` acts as a
      no-op. Use `--strip-verify` if you'd rather delete them.

What this script does NOT do
----------------------------
  * Table-name rewriting. Most adapted files keep Calcite-style
    `"scott".emp` / bare `emp` references; `scott_emp` only appears in
    `agg.iq` / `aliasing.iq`. The SQL plugin's schema resolution handles
    the rest. If your new file relies on a schema the `ansi` connection
    does not expose, you will need to rewrite table names manually.
  * Expected-result rewriting. If the OpenSearch execution path produces
    different formatting than Calcite's in-memory one (e.g. date ordering,
    decimal padding), fix up the `+---+` output blocks by hand after
    running the script.
  * `!if (fixed.*)` gating. Calcite uses these to skip tests blocked on
    specific JIRAs; keep them as-is unless you know the bug was fixed on
    our side.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# Quidem directives that terminate a `!plan` output block. `!plan` itself is
# listed so that consecutive `!plan` blocks are trimmed one at a time.
DIRECTIVE_RE = re.compile(
    r"^\s*!(ok|error|plan|if|set|use|type|verify|skip|push|pop)\b"
)

ASF_LICENSE_LINES = [
    "# Licensed to the Apache Software Foundation (ASF) under one or more",
    "# contributor license agreements.  See the NOTICE file distributed with",
    "# this work for additional information regarding copyright ownership.",
    "# The ASF licenses this file to you under the Apache License, Version 2.0",
    '# (the "License"); you may not use this file except in compliance with',
    "# the License.  You may obtain a copy of the License at",
    "#",
    "# http://www.apache.org/licenses/LICENSE-2.0",
    "#",
    "# Unless required by applicable law or agreed to in writing, software",
    '# distributed under the License is distributed on an "AS IS" BASIS,',
    "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
    "# See the License for the specific language governing permissions and",
    "# limitations under the License.",
]


def strip_plan_blocks(lines: list[str]) -> list[str]:
    """Drop every `!plan` directive and its trailing plan block.

    The plan block extends from the `!plan` line up to — but not including —
    the next Quidem directive. Blank lines and `#` comment lines inside the
    block go with it.
    """
    out: list[str] = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        if line.lstrip().startswith("!plan"):
            # Skip the !plan line and everything up to the next directive.
            i += 1
            while i < n and not DIRECTIVE_RE.match(lines[i]):
                i += 1
            continue
        out.append(line)
        i += 1
    return out


def normalise_use(lines: list[str]) -> list[str]:
    """Rewrite every `!use <anything>` to `!use ansi`."""
    pattern = re.compile(r"^(\s*!use\s+)\S+.*$")
    return [pattern.sub(r"\1ansi", line) for line in lines]


def shorten_java_prefixes(lines: list[str]) -> list[str]:
    """`java.lang.` -> `lang.`, `java.sql.` -> `sql.` inside error strings."""
    out: list[str] = []
    for line in lines:
        line = line.replace("java.lang.", "lang.")
        line = line.replace("java.sql.", "sql.")
        out.append(line)
    return out


def strip_verify(lines: list[str]) -> list[str]:
    """Optional: drop all `!verify` directives."""
    return [line for line in lines if not line.lstrip().startswith("!verify")]


def has_header(lines: list[str]) -> bool:
    """Rough test for whether the file already carries our canonical header.

    We consider the header present if we see `!use ansi` anywhere in the
    first 40 lines. That matches what every adapted file currently does
    and avoids disturbing files the user has already hand-edited.
    """
    return any("!use ansi" in line for line in lines[:40])


def build_header(basename: str) -> list[str]:
    title = f"# {basename} - adapted from Apache Calcite for OpenSearch SQL"
    return (
        [title, "#"]
        + ASF_LICENSE_LINES
        + ["", "!use ansi", "!set outputformat mysql", ""]
    )


def ensure_trailer(lines: list[str], basename: str) -> list[str]:
    """Append `# End <basename>` if it is not already the last non-blank line."""
    trailer = f"# End {basename}"
    # Strip trailing blank lines when checking, but keep them in output.
    for line in reversed(lines):
        if not line.strip():
            continue
        if line.rstrip() == trailer:
            return lines
        break
    result = list(lines)
    if result and result[-1].strip() != "":
        result.append("")
    result.append(trailer)
    return result


def adapt(content: str, basename: str, strip_verify_flag: bool) -> str:
    # Split keeping no trailing empty-string artefact from a final newline.
    lines = content.splitlines()

    lines = strip_plan_blocks(lines)
    lines = normalise_use(lines)
    lines = shorten_java_prefixes(lines)
    if strip_verify_flag:
        lines = strip_verify(lines)

    if not has_header(lines):
        lines = build_header(basename) + lines

    lines = ensure_trailer(lines, basename)

    # Re-join with a trailing newline so POSIX tools are happy.
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Adapt a Calcite upstream .iq file into the form consumed by "
            "AnsiSqlQuidemIT in this repo."
        )
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to the upstream Calcite .iq file to adapt.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help=(
            "Where to write the adapted file. Defaults to stdout. If a "
            "directory is given, the output filename is the input basename."
        ),
    )
    parser.add_argument(
        "--strip-verify",
        action="store_true",
        help="Remove all !verify directives (off by default).",
    )
    args = parser.parse_args(argv)

    if not args.input.is_file():
        print(f"adapt_iq: input not found: {args.input}", file=sys.stderr)
        return 2

    content = args.input.read_text(encoding="utf-8")
    basename = args.input.name
    adapted = adapt(content, basename, args.strip_verify)

    if args.output is None:
        sys.stdout.write(adapted)
        return 0

    dest: Path = args.output
    if dest.is_dir():
        dest = dest / basename
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(adapted, encoding="utf-8")
    # Preserve executable bit if the destination already existed.
    os.chmod(dest, 0o644)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
