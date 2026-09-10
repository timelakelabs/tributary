#!/usr/bin/env python
"""Tests for the compatibility gate.

A gate nobody has watched fail is a gate you are guessing about. These run
without git or a network:

    python .github/scripts/test_compat_gate.py
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import compat_gate  # noqa: E402


class VersionBumped(unittest.TestCase):
    """A bump is a value CHANGING, not a constant appearing."""

    def test_adding_a_new_constant_is_not_a_bump(self):
        """timelakedb#160 introduced MANIFEST_FORMAT_VERSION at 1 and was correctly
        additive. A gate that called that breaking would have demanded a
        downgrade path for a version nothing had ever written."""
        diff = "+const MANIFEST_FORMAT_VERSION: u32 = 1;\n"
        self.assertEqual(compat_gate.version_bumped(diff), [])

    def test_changing_the_value_is_a_bump(self):
        diff = ("-const MANIFEST_FORMAT_VERSION: u32 = 1;\n"
                "+const MANIFEST_FORMAT_VERSION: u32 = 2;\n")
        self.assertEqual(compat_gate.version_bumped(diff),
                         ["MANIFEST_FORMAT_VERSION"])

    def test_it_finds_them_whatever_they_are_called(self):
        diff = ("-const WAL_VERSION: u8 = 1;\n"
                "+const WAL_VERSION: u8 = 2;\n"
                "-const RETENTION_FORMAT_VERSION: u32 = 2;\n"
                "+const RETENTION_FORMAT_VERSION: u32 = 3;\n")
        self.assertEqual(compat_gate.version_bumped(diff),
                         ["RETENTION_FORMAT_VERSION", "WAL_VERSION"])

    def test_removing_a_constant_alone_is_not_a_bump(self):
        self.assertEqual(
            compat_gate.version_bumped("-const OLD_VERSION: u32 = 1;\n"), [])


class Declaration(unittest.TestCase):
    def verdict(self, body):
        m = compat_gate.COMPAT_RE.search(body)
        return m.group(1).lower() if m else None

    def test_found_anywhere_in_the_body(self):
        self.assertEqual(self.verdict("blah\n\nCompat: additive\n\nmore"),
                         "additive")

    def test_case_insensitive(self):
        self.assertEqual(self.verdict("compat: NONE"), "none")

    def test_absent_is_absent(self):
        self.assertIsNone(self.verdict("I thought about compatibility, honest"))

    def test_a_downgrade_line_needs_content(self):
        self.assertIsNone(compat_gate.DOWNGRADE_RE.search("Downgrade:\n"))
        self.assertIsNotNone(
            compat_gate.DOWNGRADE_RE.search("Downgrade: restore the backup\n"))


class PersistedList(unittest.TestCase):
    def test_the_shipped_list_parses_and_the_paths_exist(self):
        """A path that has been renamed makes the gate silently stop
        watching that format, which is the one failure mode a list like
        this has."""
        root = os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))))
        paths = compat_gate.persisted_paths(root)
        self.assertTrue(paths)
        for p in paths:
            self.assertTrue(os.path.exists(os.path.join(root, p)),
                            "{0} is listed but does not exist".format(p))

    def test_the_list_watches_itself(self):
        """Adding a new persisted format edits this file, which trips the
        gate on the commit that adds it — the one commit where somebody
        still remembers how it is meant to be read next version."""
        root = os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))))
        self.assertIn(".github/persisted-formats.txt",
                      compat_gate.persisted_paths(root))


if __name__ == "__main__":
    unittest.main()
