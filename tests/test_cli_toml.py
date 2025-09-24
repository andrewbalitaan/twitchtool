from __future__ import annotations

try:
    import tomllib  # Python 3.11+
except Exception:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

from twitchtool.cli import _serialize_toml, _dump_toml


def test_serialize_toml_string_and_list():
    # Strings: ensure escapes for backslash, quote, newline, tab
    s = 'a"b\\c\n\t'
    # Expect: quotes/backslashes escaped and control chars rendered as \n, \t
    assert _serialize_toml(s) == '"a\\"b\\\\c\\n\\t"'

    # Lists: iterate and serialize items correctly
    assert _serialize_toml([1, "x", True]) == '[1, "x", true]'


def test_dump_toml_round_trip():
    data = {
        "a": 1,
        "s": "x\ny",
        "b": {
            "arr": [1, "z"],
            "flag": False,
        },
    }
    txt = _dump_toml(data)
    # Ensure tomllib can parse what we emit and values round-trip
    parsed = tomllib.loads(txt)
    assert parsed["a"] == 1
    assert parsed["s"] == "x\ny"
    assert parsed["b"]["arr"] == [1, "z"]
    assert parsed["b"]["flag"] is False

