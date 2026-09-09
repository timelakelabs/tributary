#!/usr/bin/env python
"""Tests for the release gate.

Same rule as the compat gate: a gate nobody has watched fail is a gate you
are guessing about. The decision is a pure function of three callables, so
these run without git or a network:

    python .github/scripts/test_release_gate.py

The one exception reads `ci.yml` off disk, to check the gate's idea of what
ci ignores is the workflow's idea.
"""

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import release_gate  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
CI_YML = os.path.join(HERE, "..", "workflows", "ci.yml")

# timelakedb's 0.4.0 commit and its cancelled parent, the case that produced
# the gate (timelakedb#168). Opaque strings here; the tests do not hit git.
TAG = "678cbc0ee2b995f9668fe2c5481a3bc6aaa65ca9"
PARENT = "77bce9a482a7c361d413a4b579b34d10a7208b13"
GRAND = "0000000000000000000000000000000000000abc"


def run(number, status="completed", conclusion="success", event="push",
        branch="main", run_id=None):
    return {
        "id": run_id or number,
        "run_number": number,
        "status": status,
        "conclusion": conclusion if status == "completed" else None,
        "event": event,
        "head_branch": branch,
        "html_url": f"https://github.com/x/y/actions/runs/{number}",
    }


class World:
    """A fake repository: runs per sha, which shas are docs-only, parents."""

    def __init__(self, runs=None, docs_only=(), parents=None):
        self.runs = runs or {}
        self.docs = set(docs_only)
        self.parents = parents or {TAG: PARENT, PARENT: GRAND}

    def decide(self, sha=TAG, **kw):
        return release_gate.decide(
            sha,
            lambda s: self.runs.get(s, []),
            lambda s: s in self.docs,
            lambda s: self.parents[s],
            **kw)


class Verdicts(unittest.TestCase):

    def test_a_green_run_passes_and_names_it(self):
        ok, lines = World({TAG: [run(187)]}).decide()
        self.assertTrue(ok)
        self.assertIn("run #187 for 678cbc0 (push on main): success", lines[0])
        self.assertIn("actions/runs/187", lines[1])

    def test_a_run_still_going_is_refused_and_says_so(self):
        """The 0.4.0 shape: the release run started ten seconds after the
        ci run did, and finished first."""
        ok, lines = World({TAG: [run(187, status="in_progress")]}).decide()
        self.assertFalse(ok)
        self.assertIn("in_progress, no verdict yet", lines[0])
        self.assertIn("actions/runs/187", lines[1])
        self.assertIn("re-run this workflow", lines[2])
        self.assertIn("does not need to move", lines[2])

    def test_queued_counts_as_still_going(self):
        ok, lines = World({TAG: [run(187, status="queued")]}).decide()
        self.assertFalse(ok)
        self.assertIn("queued, no verdict yet", lines[0])

    def test_cancelled_is_refused(self):
        """77bce9a's run was killed by cancel-in-progress when the release
        commit landed nineteen minutes later. That is not a verdict."""
        ok, lines = World({PARENT: [run(186, conclusion="cancelled")]}).decide(PARENT)
        self.assertFalse(ok)
        self.assertIn("cancelled", lines[0])
        self.assertIn("actions/runs/186", lines[1])

    def test_failure_is_refused(self):
        ok, lines = World({TAG: [run(187, conclusion="failure")]}).decide()
        self.assertFalse(ok)
        self.assertIn("failure", lines[0])

    def test_the_newest_run_is_the_verdict(self):
        """A re-run keeps its number; a genuinely newer run outranks an older
        one either way."""
        ok, _ = World({TAG: [run(10, conclusion="failure"), run(11)]}).decide()
        self.assertTrue(ok)
        ok, _ = World({TAG: [run(11, conclusion="failure"), run(10)]}).decide()
        self.assertFalse(ok)

    def test_the_url_is_printed_either_way(self):
        for kw in ({}, {"conclusion": "failure"}, {"status": "in_progress"}):
            _, lines = World({TAG: [run(5, **kw)]}).decide()
            self.assertTrue(any("actions/runs/5" in l for l in lines), kw)


class NoRun(unittest.TestCase):

    def test_a_docs_only_commit_defers_to_its_parent(self):
        """ci.yml skips a markdown-only push on purpose; the last green run
        still describes the last commit that could have changed behaviour."""
        ok, lines = World({PARENT: [run(186)]}, docs_only=[TAG]).decide()
        self.assertTrue(ok)
        self.assertIn("678cbc0 has no ci.yml run and touches only files it ignores",
                      lines[0])
        self.assertIn("run #186 for 77bce9a", lines[1])

    def test_a_docs_only_commit_inherits_a_refusal_too(self):
        ok, lines = World({PARENT: [run(186, conclusion="cancelled")]},
                          docs_only=[TAG]).decide()
        self.assertFalse(ok)
        self.assertIn("cancelled", lines[1])

    def test_an_untested_code_commit_is_refused(self):
        """A tag on a branch that never went through a pull request."""
        ok, lines = World().decide()
        self.assertFalse(ok)
        self.assertIn("never tested", lines[0])

    def test_the_walk_is_bounded(self):
        parents = {TAG: PARENT, PARENT: GRAND, GRAND: TAG}  # a loop, to be rude
        ok, lines = World(docs_only=[TAG, PARENT, GRAND], parents=parents
                          ).decide(max_walk=3)
        self.assertFalse(ok)
        self.assertIn("gave up after 3", lines[-1])


class Ignored(unittest.TestCase):

    def test_matches_what_ci_yml_says_it_ignores(self):
        """The gate's list and the workflow's list are the same list, or the
        gate is quietly gating on a rule ci does not follow."""
        with open(CI_YML, encoding="utf-8") as f:
            text = f.read()
        block = re.search(r"paths-ignore:\n((?:[ \t]+- '[^']*'\n)+)", text)
        self.assertIsNotNone(block, "no paths-ignore block in ci.yml")
        patterns = tuple(re.findall(r"- '([^']*)'", block.group(1)))
        self.assertEqual(patterns, release_gate.CI_IGNORED)

    def test_the_shapes_in_use(self):
        for path in ("README.md", "docs/evidence/P0-1-ci.md", "LICENSE"):
            self.assertTrue(release_gate.ignored(path), path)
        for path in ("Cargo.toml", "crates/wal/src/lib.rs", "site/docs/index.html",
                     "crates/x/LICENSE", ".github/workflows/ci.yml", "README.md.bak"):
            self.assertFalse(release_gate.ignored(path), path)


if __name__ == "__main__":
    unittest.main()
