import pandas as pd
from dagster import (
    AssetCheckResult,
    Definitions,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

import db


def build_high_value_eur_report(
    orders: pd.DataFrame,
    products: pd.DataFrame,
    exchange_rates: pd.DataFrame,
    predictions: pd.DataFrame,
) -> pd.DataFrame:
    eur_rates = exchange_rates[exchange_rates["quote_currency"] == "EUR"]
    if eur_rates.empty:
        raise ValueError("No USD->EUR rate found in exchange_rates table")
    usd_to_eur = eur_rates.iloc[0]["rate"]

    merged = orders.merge(products, on="product_id", how="inner")
    merged["total_eur"] = merged["quantity"] * merged["price"] * usd_to_eur
    order_totals = merged.groupby("order_id")["total_eur"].sum().reset_index()

    pred_agg = (
        predictions.groupby("order_id")
        .agg(predicted_high_value=("predicted_label", "max"), avg_probability=("probability", "mean"))
        .reset_index()
    )

    report = order_totals.merge(pred_agg, on="order_id", how="inner")
    return report[
        ["order_id", "total_eur", "predicted_high_value", "avg_probability"]
    ].sort_values("order_id").reset_index(drop=True)


@asset
def high_value_orders_eur_report() -> pd.DataFrame:
    orders = db.read_table("orders")
    products = db.read_table("products")
    exchange_rates = db.read_table("exchange_rates")
    predictions = db.read_table("order_value_predictions")
    return build_high_value_eur_report(orders, products, exchange_rates, predictions)


@asset
def high_value_orders_eur_report_table(high_value_orders_eur_report: pd.DataFrame) -> int:
    return db.load_table(high_value_orders_eur_report, "high_value_orders_eur_report")


@asset_check(asset=high_value_orders_eur_report)
def report_has_no_duplicate_orders(high_value_orders_eur_report: pd.DataFrame) -> AssetCheckResult:
    num_duplicates = int(high_value_orders_eur_report["order_id"].duplicated().sum())
    return AssetCheckResult(
        passed=num_duplicates == 0,
        metadata={"num_duplicate_orders": num_duplicates},
    )


refresh_reporting_job = define_asset_job(name="refresh_reporting_job")

refresh_reporting_daily = ScheduleDefinition(
    name="refresh_reporting_daily",
    job=refresh_reporting_job,
    cron_schedule="0 7 * * *",
)

defs = Definitions(
    assets=[high_value_orders_eur_report, high_value_orders_eur_report_table],
    asset_checks=[report_has_no_duplicate_orders],
    jobs=[refresh_reporting_job],
    schedules=[refresh_reporting_daily],
)
