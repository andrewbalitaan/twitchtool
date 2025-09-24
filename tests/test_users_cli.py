from __future__ import annotations

from pathlib import Path

import pytest

from twitchtool.users_cli import add_users, list_users, remove_users


def test_add_and_list(tmp_path, capsys):
    users_file = tmp_path / "users.txt"

    rc = add_users(users_file, ["djalpha", "djbeta"])
    assert rc == 0
    out, err = capsys.readouterr()
    assert "Added 2 user(s): djalpha, djbeta" in out
    assert err == ""
    assert users_file.read_text().strip().splitlines() == ["djalpha", "djbeta"]

    rc = list_users(users_file)
    assert rc == 0
    out, err = capsys.readouterr()
    assert "Users file" in out
    assert "djalpha" in out
    assert "djbeta" in out
    assert err == ""


def test_add_skips_invalid_and_duplicates(tmp_path, capsys):
    users_file = tmp_path / "users.txt"
    users_file.write_text("djalpha\n")

    rc = add_users(users_file, ["djalphA", "bad!", "djsigma"])
    assert rc == 1  # invalid username triggers non-zero
    out, err = capsys.readouterr()
    assert "Skipped existing user(s): djalphA" in out
    assert "Added 1 user(s): djsigma" in out
    assert "Invalid username(s): bad!" in err
    assert users_file.read_text().strip().splitlines() == ["djalpha", "djsigma"]


def test_remove_users(tmp_path, capsys):
    users_file = tmp_path / "users.txt"
    users_file.write_text("djalpha\ndjbeta\ndjgamma\n")

    rc = remove_users(users_file, ["DJALPHA", "unknown"])
    # Removing unknown should succeed (ignored), but we removed djalphaa only
    assert rc == 0
    out, err = capsys.readouterr()
    assert "Removed 1 user(s): djalpha" in out
    assert err == ""
    assert users_file.read_text() == "djbeta\ndjgamma\n"

    # Removing remaining users empties file
    rc = remove_users(users_file, ["djbeta", "djgamma"])
    assert rc == 0
    out, err = capsys.readouterr()
    assert "Removed 2 user(s): djbeta, djgamma" in out
    assert users_file.read_text() == ""

    rc = remove_users(users_file, ["djbeta"])
    assert rc == 1
    out, err = capsys.readouterr()
    assert "No matching users found to remove." in out
    assert err == ""
