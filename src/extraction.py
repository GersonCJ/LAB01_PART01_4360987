# --------- Extract the raw data out of GitHub repo: https://github.com/owid/co2-data
import pandas as pd


def extract(url: str, url_meta: str):
    """Extract data from url and save it as csv file"""
    raw_df = pd.read_csv(url)
    raw_df.to_csv("data/raw/owid_co2_raw_data.csv", index=False)

    raw_meta = pd.read_csv(url_meta)
    raw_meta.to_csv("data/raw/owid_co2_codebook.csv", index=False)



