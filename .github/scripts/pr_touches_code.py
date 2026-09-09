#!/usr/bin/env python
"""Does this pull request change anything `ci.yml` would run for?

Required status checks and `paths-ignore` do not mix. A workflow that is
skipped by path filtering never reports, so its checks sit "expected"
forever and a pull request that requires them can never merge (GitHub's
own troubleshooting page says so, twice). Five docs-only pull requests
merged across the two product repositories between 2026-08-13 and
2026-09-09 with no ci run at all, two of them here; under required checks
every one of them would have been stuck (timelakedb#169).

A job skipped by an `if` conditional, on the other hand, reports success.
So `ci.yml` runs on every pull request, this decides in seconds whether
anything the expensive jobs care about changed, and they condition on the
answer. A docs-only pull request gets a run in which everything but this
is skipped, which is a green run, which merges.

The list of what ci ignores is `release_gate.CI_IGNORED`, the one copy,
kept equal to the workflow's own `paths-ignore` by a test. Do not add a
second list here.

    python .github/scripts/pr_touches_code.py --base <sha> [--head HEAD]

Writes `code=true|false` to $GITHUB_OUTPUT when set.
"""

import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from release_gate import ignored  # noqa: E402


def code_files(files):
    """The changed paths ci.yml would run for. Empty means docs-only."""
    return [f for f in files if f and not ignored(f)]


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base", required=True, help="base commit of the pull request")
    ap.add_argument("--head", default="HEAD")
    args = ap.parse_args()

    files = subprocess.check_output(
        ["git", "diff", "--name-only", args.base, args.head], text=True).split("\n")
    files = [f for f in files if f]
    code = code_files(files)

    if code:
        print(f"code=true: {len(code)} of {len(files)} changed paths are ones ci runs for")
        for f in code[:20]:
            print("  " + f)
        if len(code) > 20:
            print(f"  ... {len(code) - 20} more")
    else:
        print(f"code=false: {len(files)} changed paths, all of them markdown or LICENSE;"
              " the expensive jobs are skipped and report success")
        for f in files[:20]:
            print("  " + f)

    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(f"code={'true' if code else 'false'}\n")


if __name__ == "__main__":
    main()
