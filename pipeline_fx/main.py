import pandas as pd
from dagster import Definitions, ScheduleDefinition, asset, define_asset_job

import db
import source


@asset
def raw_exchange_rates() -> pd.DataFrame:
    payload = source.fetch_latest_rates(base="USD")
    rows = [
        {"base_currency": "USD", "quote_currency": currency, "rate": rate}
        for currency, rate in payload["rates"].items()
    ]
    return pd.DataFrame(rows)


@asset
def exchange_rates_table(raw_exchange_rates: pd.DataFrame) -> int:
    return db.load_table(raw_exchange_rates, "exchange_rates")


@asset(deps=[exchange_rates_table])
def orders_in_eur() -> pd.DataFrame:
    engine = db.get_engine()
    orders = pd.read_sql("SELECT * FROM orders", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    exchange_rates = pd.read_sql("SELECT * FROM exchange_rates", engine)

    eur_rates = exchange_rates[exchange_rates["quote_currency"] == "EUR"]
    if eur_rates.empty:
        raise ValueError("No USD->EUR rate found in exchange_rates table")
    usd_to_eur = eur_rates.iloc[0]["rate"]

    merged = orders.merge(products, on="product_id", how="inner")
    merged["total_usd"] = merged["quantity"] * merged["price"]
    merged["total_eur"] = merged["total_usd"] * usd_to_eur

    return merged[
        ["order_id", "product_id", "customer_id", "quantity", "total_usd", "total_eur"]
    ]


refresh_fx_job = define_asset_job(name="refresh_fx_job")

refresh_fx_daily = ScheduleDefinition(
    name="refresh_fx_daily",
    job=refresh_fx_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[raw_exchange_rates, exchange_rates_table, orders_in_eur],
    jobs=[refresh_fx_job],
    schedules=[refresh_fx_daily],
)
