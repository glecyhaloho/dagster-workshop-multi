import os

import pandas as pd
from sqlalchemy import create_engine


def get_engine():
    user = os.environ["WAREHOUSE_USER"]
    password = os.environ["WAREHOUSE_PASSWORD"]
    host = os.environ["WAREHOUSE_HOST"]
    port = os.environ["WAREHOUSE_PORT"]
    db_name = os.environ["WAREHOUSE_DB"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")


def load_table(df: pd.DataFrame, table_name: str) -> int:
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    return len(df)


def read_table(table_name: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)
