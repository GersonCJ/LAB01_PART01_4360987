from pathlib import Path
import pandas as pd
import src.extraction as ext

# ----------------- Pandas configs - Temp
# Show every single column
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', 80)

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
raw_data = pd.read_csv("data/raw/owid_co2_raw_data.csv")
print(raw_data.info())
print(raw_data.describe())
print(raw_data.isna().sum())

