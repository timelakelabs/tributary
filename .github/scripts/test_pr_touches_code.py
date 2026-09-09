#!/usr/bin/env python
"""Tests for the docs-only decision. No git, no network:

    python .github/scripts/test_pr_touches_code.py
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pr_touches_code  # noqa: E402


class CodeFiles(unittest.TestCase):

    def test_markdown_and_license_alone_are_docs_only(self):
        self.assertEqual(
            pr_touches_code.code_files(["README.md", "docs/ROADMAP.md", "LICENSE"]), [])

    def test_one_source_file_makes_it_code(self):
        files = ["README.md", "crates/wal/src/lib.rs"]
        self.assertEqual(pr_touches_code.code_files(files), ["crates/wal/src/lib.rs"])

    def test_the_workflow_itself_is_code(self):
        """A change to ci.yml must run ci.yml."""
        self.assertEqual(pr_touches_code.code_files([".github/workflows/ci.yml"]),
                         [".github/workflows/ci.yml"])

    def test_empty_is_docs_only(self):
        self.assertEqual(pr_touches_code.code_files([]), [])
        self.assertEqual(pr_touches_code.code_files([""]), [])


if __name__ == "__main__":
    unittest.main()
