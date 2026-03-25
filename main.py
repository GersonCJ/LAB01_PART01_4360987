from pathlib import Path
import pandas as pd
import src.extraction as ext
import src.transformation as trf
import Constants.pandas_options

# Defining Paths
bronze_path = Path("data/raw")
silver_path = Path("data/silver")

# Ensuring Path exists
bronze_path.mkdir(exist_ok=True, parents=True)
silver_path.mkdir(exist_ok=True, parents=True)

# ------------------ Extraction using commit a499dd34c1372468f2335a370c5dd13cc3a72d90

url = "https://raw.githubusercontent.com/owid/co2-data/a499dd34c1372468f2335a370c5dd13cc3a72d90/owid-co2-data.csv"
url_metadata = "https://raw.githubusercontent.com/owid/co2-data/a499dd34c1372468f2335a370c5dd13cc3a72d90/owid-co2-codebook.csv"

if not any(bronze_path.iterdir()):
    print("Starting extraction ...")
    ext.extract(url, url_metadata)
else:
    print("Data already available. Skipping extraction ...")

# -------- Initial Description of the Data (Pre-Transformation)

metadata = trf.load_bronze("data/raw/owid_co2_codebook.csv")
raw_data = trf.load_bronze("data/raw/owid_co2_raw_data.csv")
print(f"Characterization - Basic infos (Columns, Non-nulls, Dtype of columns):\n{raw_data.info()}")
print(f"Characterization - Data Descriptions:\n{raw_data.describe()}")
print(f"Characterization - Nulls Count:\n{raw_data.isna().sum()}")

# ------------------- Transformations

# 1. Drop duplicates and Drop Data where Country, Year and Population are Nan.
cleaned_data = trf.data_cleanse(raw_data)

# 2. Standardize data - Correct types, add to columns the units from metadata
standardized_data = trf.standardizing_data(
    cleaned_data, trf.obtaining_units_from_meta(metadata)
)

# 3. Give Kosovo a Fake Iso, so it is included with the other nations in the Nations dataframe.
before_split_df = trf.input_special_isos(standardized_data)

# 4. Split the dataset into Two Datasets - One containing the aggregates, One containing only countries.
national_df, aggregate_df = trf.silver_split(before_split_df)

# 5. If no data in data/silver, save both the national and aggregate dfs in the silver_path in parquet, otherwise skip.
if not any(silver_path.iterdir()):
    print("Starting to save ...")
    trf.save_to_silver(national_df, "National_table_parquet", silver_path)
    trf.save_to_silver(aggregate_df, "Aggregate_table_parquet", silver_path)
else:
    print("Data already available. Saving skipped ...")

# 6. -------- Initial Description of the Data (Pre-Transformation)
print(f"Characterization - Basic infos (Columns, Non-nulls, Dtype of columns):\n{national_df.info()}")
print(f"Characterization - Data Descriptions:\n{national_df.describe()}")
print(f"Characterization - Nulls Count:\n{national_df.isna().sum()}")

# 7.
