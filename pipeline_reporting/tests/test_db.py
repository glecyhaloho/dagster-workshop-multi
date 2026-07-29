from unittest.mock import MagicMock, patch

import pandas as pd

import db


def test_get_engine_builds_expected_connection_string(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_USER", "u")
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "p")
    monkeypatch.setenv("WAREHOUSE_HOST", "h")
    monkeypatch.setenv("WAREHOUSE_PORT", "5432")
    monkeypatch.setenv("WAREHOUSE_DB", "d")

    with patch("db.create_engine") as mock_create_engine:
        db.get_engine()

    mock_create_engine.assert_called_once_with("postgresql+psycopg2://u:p@h:5432/d")


def test_load_table_writes_dataframe_and_returns_row_count(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_USER", "u")
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "p")
    monkeypatch.setenv("WAREHOUSE_HOST", "h")
    monkeypatch.setenv("WAREHOUSE_PORT", "5432")
    monkeypatch.setenv("WAREHOUSE_DB", "d")

    df = pd.DataFrame({"a": [1, 2, 3]})
    fake_engine = MagicMock()

    with patch("db.create_engine", return_value=fake_engine):
        with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
            row_count = db.load_table(df, "my_table")

    mock_to_sql.assert_called_once_with("my_table", fake_engine, if_exists="replace", index=False)
    assert row_count == 3


def test_read_table_reads_dataframe_from_table(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_USER", "u")
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "p")
    monkeypatch.setenv("WAREHOUSE_HOST", "h")
    monkeypatch.setenv("WAREHOUSE_PORT", "5432")
    monkeypatch.setenv("WAREHOUSE_DB", "d")

    fake_engine = MagicMock()
    fake_df = pd.DataFrame({"a": [1, 2]})

    with patch("db.create_engine", return_value=fake_engine):
        with patch("db.pd.read_sql", return_value=fake_df) as mock_read_sql:
            result = db.read_table("my_table")

    mock_read_sql.assert_called_once_with("SELECT * FROM my_table", fake_engine)
    pd.testing.assert_frame_equal(result, fake_df)
