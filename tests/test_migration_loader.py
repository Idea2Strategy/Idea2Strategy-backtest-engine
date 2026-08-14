from __future__ import annotations

import pytest

from conftest import _split_copy_from_stdin


def test_split_copy_from_stdin_preserves_sql_and_copy_payload() -> None:
    script = (
        "CREATE TABLE example (id integer, label text);\n"
        "COPY example (id, label) FROM stdin;\n"
        "1\tfirst\n"
        "2\tsecond\\tvalue\n"
        "\\.\n"
        "ALTER TABLE example ADD PRIMARY KEY (id);\n"
    )

    assert list(_split_copy_from_stdin(script)) == [
        ("sql", "CREATE TABLE example (id integer, label text);\n"),
        (
            "copy",
            ("COPY example (id, label) FROM stdin;", "1\tfirst\n2\tsecond\\tvalue\n"),
        ),
        ("sql", "ALTER TABLE example ADD PRIMARY KEY (id);\n"),
    ]


def test_split_copy_from_stdin_rejects_an_unterminated_payload() -> None:
    with pytest.raises(ValueError, match="unterminated COPY FROM stdin block"):
        list(_split_copy_from_stdin("COPY example (id) FROM stdin;\n1\n"))
