from Constants import pandas_options, fact_const_columns
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, Engine, text
import os
import pandas as pd
import re
import urllib.parse

env_path = Path(__file__).resolve().parent.parent / '.env'
print(env_path)
load_dotenv(dotenv_path=env_path)


def load_silver(path: str) -> pd.DataFrame:
    # Accessing transformed (silver) data
    return pd.read_parquet(path)


def clean_column_name(col):
    col = col.lower()
    col = col.replace("($)", "usd")
    col = col.replace("(%)", "prct")
    col = col.replace("(mt)", "mt")
    col = col.replace("(°c)", "degrees_c")
    col = col.replace("(people)", "people")
    col = col.replace("(t/person)", "t_per_person")
    col = col.replace("(kg/$)", "kg_per_usd")
    col = col.replace("(kg/kwh)", "kg_per_kwh")

    # Replace ANY non-alphanumeric character (like $, %, /, (, ), spaces) with an underscore
    col = re.sub(r'[^a-z0-9]+', '_', col)

    # 3. Clean up: Remove leading/trailing underscores and change '___' to '_'
    col = col.strip('_')
    col = re.sub(r'_+', '_', col)

    return col


def logical_split(df: pd.DataFrame) -> [pd.DataFrame]:

    # Sanitize the column names for SQL query
    df.columns = [clean_column_name(c) for c in df.columns]
    try:
        df_emissions_main = df[fact_const_columns.fact_emissions_main].copy()
        df_co2_consumption_main = df[fact_const_columns.fact_consumption_co2_main].copy()
        df_emission_sources = df[fact_const_columns.fact_emission_sources].copy()
        df_non_ghg = df[fact_const_columns.fact_non_co2_ghg].copy()
        df_climate_impact = df[fact_const_columns.fact_climate_impact].copy()
    except KeyError as e:
        print(f"Still missing columns: {e}")
        print(df.columns)

    return df_emissions_main, df_co2_consumption_main, df_emission_sources, df_non_ghg, df_climate_impact


def gold_filtering(sliced_df: pd.DataFrame) -> pd.DataFrame:
    # Using as base the backbone of the columns
    backbone = ["country", "year", "iso_code", "population_people"]
    actual_backbone = [col for col in backbone if col in sliced_df.columns]
    remaining_columns = sliced_df.columns.difference(actual_backbone)
    # Remove lines where everything is Nan except for the backbone.
    filtered_df = sliced_df.dropna(subset=remaining_columns, how='all')
    return filtered_df


def push_to_db(df: pd.DataFrame, table_name: str, engine: Engine, schema: str) -> None:

    # 1. Empty the table but keep the structure and types
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
        conn.commit()

    try:
        print("Starting upload...")
        df.to_sql(
            name=table_name,
            con=engine,
            schema="co2_project",
            if_exists="append",
            index=False,
            chunksize=10000
        )
        print("Success ! Your Gold Layer is now populated with data.")
    except Exception as e:
        print(f"Error: {e}")


def run_query(sql_query: str, engine: Engine, params=None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(sql_query, conn, params=params)


if __name__ == '__main__':
    national_silver = load_silver("../data/silver/National_table_parquet.parquet")
    # print(national_silver)
    agg_silver = load_silver("../data/silver/Aggregate_table_parquet.parquet")
    agg_silver["country"] = agg_silver["country"].str.strip()

    emissions_main, consumptions_main, emission_sources_main, non_co2_ghg_main, climate_impact_main = logical_split(national_silver)
    emissions_agg, consumptions_agg, emission_sources_agg, non_co2_ghg_agg, climate_impact_agg = logical_split(agg_silver)

    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    db_name = os.getenv("DB_NAME")

    # 3. Debugging (Crucial step!)
    if not all([user, password, db_name]):
        print(f"ERROR: Missing environment variables! Checked path: {env_path}")
        # This will tell you exactly what Python sees (or doesn't see)
    else:
        print(f"Environment variables loaded for user: {user}")

    # Encode the password to handle special characters (@, !, #, etc...)
    encoded_password = urllib.parse.quote_plus(password)
    engine = create_engine(f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}")
    """
    push_to_db(emissions_main, "fact_emissions", engine, schema="co2_project")
    push_to_db(consumptions_main, "fact_consumption", engine, schema="co2_project")
    push_to_db(emission_sources_main, "fact_emission_sources", engine, schema="co2_project")
    push_to_db(non_co2_ghg_main, "fact_non_co2_ghg", engine, schema="co2_project")
    push_to_db(climate_impact_main, "fact_climate_impact", engine, schema="co2_project")
    
    push_to_db(emissions_agg, "agg_emissions", engine, schema="co2_project")
    push_to_db(consumptions_agg, "agg_consumption", engine, schema="co2_project")
    push_to_db(emission_sources_agg, "agg_emission_sources", engine, schema="co2_project")
    push_to_db(non_co2_ghg_agg, "agg_non_co2_ghg", engine, schema="co2_project")
    push_to_db(climate_impact_agg, "agg_climate_impact", engine, schema="co2_project")
    """
    query1 = "select * from co2_project.fact_emissions;"
    df = run_query(query1, engine)
    print(df)
