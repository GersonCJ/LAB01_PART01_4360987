# --------- Extract the raw data out of Git Hub repo: https://github.com/owid/co2-data
import pandas as pd


def extract(url: str):
    raw_df = pd.read_csv(url)
    raw_df.to_csv("data/1_Bronze/owid_co2_raw_data.csv", index=False)



