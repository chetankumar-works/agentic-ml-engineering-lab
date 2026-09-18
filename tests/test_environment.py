"""Sanity checks for the pinned development environment (Milestone 0)."""

import sys


def test_python_version_is_pinned_312() -> None:
    assert sys.version_info.major == 3
    assert sys.version_info.minor == 12


def test_core_stdlib_modules_available() -> None:
    # Guards against a standalone interpreter missing ssl/sqlite3 support,
    # which some minimal Python builds omit.
    import bz2  # noqa: F401
    import lzma  # noqa: F401
    import sqlite3  # noqa: F401
    import ssl  # noqa: F401
