import pandas as pd
import requests
import datetime
from tqdm import tqdm
import utilities
import os


def asset_price(fsym, tsym, limit=10, freq="days", market="ccix"):

    if limit <= 2000:
        url = (
            "https://data-api.cryptocompare.com/index/cc/v1/historical/"
            f"{freq}?market={market}&instrument={fsym.upper()}-{tsym.upper()}"
            f"&limit={limit}&aggregate=1&fill=true&apply_mapping=true&"
            f"response_format=JSON&api_key={api_key}"
        )
        df = pd.DataFrame(requests.get(url).json()["Data"])
        df.index = pd.to_datetime(df.TIMESTAMP, unit="s")

    if limit > 2000:
        df_final = pd.DataFrame()
        tots = datetime.datetime.now().timestamp()
        for i in tqdm(range(100)):
            url = (
                "https://data-api.cryptocompare.com/index/cc/v1/historical/"
                f"{freq}?market={market}&instrument={fsym.upper()}-{tsym.upper()}"
                f"&limit=2000&to_ts={tots}&aggregate=1&fill=true&apply_mapping=true&"
                f"response_format=JSON&api_key={api_key}"
            )
            df = pd.DataFrame(requests.get(url).json()["Data"])
            df_final = pd.concat([df_final, df])
            tots = df_final.TIMESTAMP.min()

        df = df_final
        df.index = pd.to_datetime(df.TIMESTAMP, unit="s")
        df.drop_duplicates(inplace=True)
        df.drop("TIMESTAMP", axis=1, inplace=True)

    return df


if __name__ == "__main__":

    api_key = os.getenv("api_key")
    GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
    dataset_id = os.getenv("dataset_id")
    table_id = "cex-pricing"

    df_eth = asset_price("ETH", "USDC", limit=20000, freq="minutes")
    df_sol = asset_price("SOL", "USDC", limit=20000, freq="minutes")
    df_bnb = asset_price("BNB", "USDC", limit=20000, freq="minutes")

    utilities.load_to_table(df_sol, dataset_id, table_id, GOOGLE_CREDENTIALS_PATH)
    utilities.load_to_table(df_eth, dataset_id, table_id, GOOGLE_CREDENTIALS_PATH)
    utilities.load_to_table(df_bnb, dataset_id, table_id, GOOGLE_CREDENTIALS_PATH)
