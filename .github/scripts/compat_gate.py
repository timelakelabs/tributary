#!/usr/bin/env python
"""Make a change to an on-disk format answer for itself before it merges.

The failure this exists for, in one sentence: for three releases every
TimeLakeDB manifest field was added with `#[serde(default)]`, which reads
correctly forward and is silently wrong backward, and nobody was asked
about it once (timelakedb#160). Tributary's checkpoint is built the same
way, field by field with `#[serde(default)]`, and its spool segments are
what decides whether an agent upgrade loses buffered data (timelakedb#173).
This is a copy of TimeLakeDB's gate and is meant to stay one.

So: if a pull request touches a file that owns a persisted format, its body
must say what that does to compatibility. Not a checkbox — a declaration,
cross-checked against the diff:

    Compat: none        nothing persisted changed shape
    Compat: additive    an older binary still reads new data CORRECTLY
    Compat: breaking    it does not, and a format version is bumped

"Additive" is the one worth being careful about, and the one #160 got
wrong for three releases in TimeLakeDB. A field an older reader silently drops is
additive only if dropping it is harmless. If the field carries an
instruction — a delete, a drop, a retirement — then an older reader
ignoring it does the WRONG THING quietly, and that is `breaking` however
optional the field looks.

Two cross-checks, because a declaration nobody verifies is a checkbox with
extra steps:

  * `breaking` requires a *_VERSION constant to move in the same diff.
    Declaring the break without bumping leaves the next binary with no way
    to detect it.
  * a *_VERSION constant moving requires `breaking`. Bumping while claiming
    `none` or `additive` means one of the two is a lie.

And `breaking` requires a `Downgrade:` line, because the pull request that
introduces an incompatibility is the only place that still knows how the
new thing maps onto the old one. Six months later nobody does.

    python .github/scripts/compat_gate.py --base <sha> --head <sha>

Body comes from the PR_BODY environment variable, so no quoting of
arbitrary prose through a shell.
"""

import argparse
import os
import re
import subprocess
import sys

VERDICTS = ("none", "additive", "breaking")
COMPAT_RE = re.compile(r"^\s*Compat:\s*(\w+)", re.MULTILINE | re.IGNORECASE)
DOWNGRADE_RE = re.compile(r"^\s*Downgrade:\s*(\S.*)$", re.MULTILINE | re.IGNORECASE)
# A version constant, however it is spelled: WAL_VERSION,
# MANIFEST_FORMAT_VERSION, RETENTION_FORMAT_VERSION, ...
VERSION_CONST_RE = re.compile(
    r"^(?P<sign>[+-]).*\b(?P<name>[A-Z][A-Z0-9_]*VERSION)\b\s*:\s*\w+\s*=", re.MULTILINE)


def version_bumped(diff):
    """Names of version constants whose VALUE changed in this diff.

    A bump is the same constant on both a `-` and a `+` line. Merely adding
    one is not a bump and must not be treated as one: #160 introduced
    MANIFEST_FORMAT_VERSION at 1 and was correctly additive, and a gate that
    called that "breaking" would have demanded a downgrade path for a
    version nothing had ever written.
    """
    removed, added = set(), set()
    for m in VERSION_CONST_RE.finditer(diff):
        (added if m.group("sign") == "+" else removed).add(m.group("name"))
    return sorted(removed & added)


def run(args):
    r = subprocess.run(args, capture_output=True)
    if r.returncode != 0:
        sys.stderr.write(r.stderr.decode("utf-8", "replace"))
        raise SystemExit("git failed: {0}".format(" ".join(args)))
    return r.stdout.decode("utf-8", "replace")


def persisted_paths(root):
    """The declared list, comments and blank lines stripped."""
    out = []
    path = os.path.join(root, ".github", "persisted-formats.txt")
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if line:
                out.append(line)
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", required=True)
    p.add_argument("--head", default="HEAD")
    p.add_argument("--root", default=".")
    args = p.parse_args()

    declared = persisted_paths(args.root)
    changed = [f for f in run(["git", "diff", "--name-only",
                               "{0}...{1}".format(args.base, args.head)]).split("\n") if f]
    touched = sorted(set(changed) & set(declared))

    if not touched:
        print("compat: no persisted format touched by this change — nothing to declare.")
        print("        (the list is .github/persisted-formats.txt)")
        return 0

    print("compat: this change touches a persisted format:")
    for t in touched:
        print("          {0}".format(t))
    print()

    body = os.environ.get("PR_BODY", "")
    diff = run(["git", "diff", "-U0",
                "{0}...{1}".format(args.base, args.head), "--"] + declared)
    bumped = version_bumped(diff)
    version_moved = bool(bumped)

    m = COMPAT_RE.search(body)
    if not m:
        return fail(
            "the pull request body does not declare what this does to "
            "compatibility.",
            touched, version_moved)

    verdict = m.group(1).lower()
    if verdict not in VERDICTS:
        return fail(
            "'Compat: {0}' is not one of {1}.".format(verdict, ", ".join(VERDICTS)),
            touched, version_moved)

    if verdict == "breaking" and not version_moved:
        return fail(
            "declared 'Compat: breaking', but no *_VERSION constant moved in "
            "this diff. A break an older binary cannot DETECT is the timelakedb#160 bug "
            "with a note attached: bump the format version so the next "
            "version can refuse rather than guess.",
            touched, version_moved)

    if verdict != "breaking" and version_moved:
        return fail(
            "a *_VERSION constant moved, but the declaration says "
            "'Compat: {0}'. One of the two is wrong. A version only moves "
            "when an older reader would MIS-apply the new data — if that is "
            "what is happening, this is 'breaking'; if it is not, the bump "
            "should not be here.".format(verdict),
            touched, version_moved)

    if verdict == "breaking" and not DOWNGRADE_RE.search(body):
        return fail(
            "declared 'Compat: breaking' with no 'Downgrade:' line. This "
            "pull request is the only place that still knows how the new "
            "shape maps onto the old one; in six months nobody will. Say how "
            "an operator gets back, even if the answer is 'restore the "
            "pre-upgrade backup, there is no in-place path'.",
            touched, version_moved)

    print("compat: declared '{0}'{1}. OK.".format(
        verdict, " with a downgrade path" if verdict == "breaking" else ""))
    return 0


def fail(why, touched, version_moved):
    print("::error::compatibility gate: {0}".format(why))
    print()
    print("  Add a line to the pull request body:")
    print()
    print("      Compat: none | additive | breaking")
    print()
    print("  none      nothing persisted changed shape")
    print("  additive  an older binary still reads new data CORRECTLY.")
    print("            Careful here: a field an older reader silently drops is")
    print("            additive only if dropping it is HARMLESS. If it carries")
    print("            an instruction — a delete, a drop, a retirement — then")
    print("            ignoring it does the wrong thing quietly, and that is")
    print("            'breaking' however optional the field looks. That is")
    print("            exactly what timelakedb#160 was.")
    print("  breaking  it does not. Bump the format version, and add:")
    print()
    print("      Downgrade: <how an operator gets back, or that they cannot>")
    print()
    print("  Touched: {0}".format(", ".join(touched)))
    print("  A version constant CHANGED VALUE in this diff: {0}".format(
        "yes" if version_moved else "no"))
    print("  (adding a new constant is not a bump; the same name must appear")
    print("   on both a - and a + line)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
