from unittest.mock import patch

import pandas as pd
from dagster import materialize

import db
from main import (
    build_high_value_eur_report,
    high_value_orders_eur_report,
    high_value_orders_eur_report_table,
    report_has_no_duplicate_orders,
)

ORDERS = pd.DataFrame(
    [
        {"order_id": 100, "product_id": 1, "quantity": 2},
        {"order_id": 100, "product_id": 2, "quantity": 1},
        {"order_id": 101, "product_id": 1, "quantity": 1},
    ]
)
PRODUCTS = pd.DataFrame(
    [
        {"product_id": 1, "price": 10.0},
        {"product_id": 2, "price": 5.0},
    ]
)
EXCHANGE_RATES = pd.DataFrame(
    [{"base_currency": "USD", "quote_currency": "EUR", "rate": 0.9}]
)
PREDICTIONS = pd.DataFrame(
    [
        {"order_id": 100, "predicted_label": 1, "probability": 0.8, "actual_label": 1},
        {"order_id": 100, "predicted_label": 0, "probability": 0.6, "actual_label": 1},
        {"order_id": 101, "predicted_label": 0, "probability": 0.3, "actual_label": 0},
    ]
)


def test_build_high_value_eur_report_joins_and_aggregates_per_order():
    report = build_high_value_eur_report(ORDERS, PRODUCTS, EXCHANGE_RATES, PREDICTIONS)

    assert list(report["order_id"]) == [100, 101]

    row_100 = report.loc[report["order_id"] == 100].iloc[0]
    assert row_100["total_eur"] == (2 * 10.0 + 1 * 5.0) * 0.9
    assert row_100["predicted_high_value"] == 1
    assert row_100["avg_probability"] == 0.7

    row_101 = report.loc[report["order_id"] == 101].iloc[0]
    assert row_101["total_eur"] == (1 * 10.0) * 0.9
    assert row_101["predicted_high_value"] == 0
    assert row_101["avg_probability"] == 0.3


def test_build_high_value_eur_report_raises_without_eur_rate():
    no_eur_rates = pd.DataFrame([{"base_currency": "USD", "quote_currency": "GBP", "rate": 0.8}])
    try:
        build_high_value_eur_report(ORDERS, PRODUCTS, no_eur_rates, PREDICTIONS)
    except ValueError as exc:
        assert "EUR" in str(exc)
    else:
        raise AssertionError("expected ValueError when no USD->EUR rate is present")


def test_reporting_pipeline_produces_report_and_passes_quality_check():
    loaded = {}

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    def fake_read_table(table_name: str) -> pd.DataFrame:
        return {
            "orders": ORDERS,
            "products": PRODUCTS,
            "exchange_rates": EXCHANGE_RATES,
            "order_value_predictions": PREDICTIONS,
        }[table_name]

    with patch.object(db, "read_table", side_effect=fake_read_table), patch.object(
        db, "load_table", side_effect=fake_load_table
    ):
        result = materialize(
            [high_value_orders_eur_report, high_value_orders_eur_report_table, report_has_no_duplicate_orders]
        )

    assert result.success
    assert len(loaded["high_value_orders_eur_report"]) == 2

    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 1
    assert evaluations[0].passed is True
