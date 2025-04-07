import pandas as pd
from google.cloud import bigquery
import utilities
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")


def read_and_process_csv(file_path):
    """
    Reads a CSV file and processes it into a DataFrame with orderId, evt_time, and chain.
    Args: file_path (str): Path to the CSV file.
    Returns: pd.DataFrame: Processed DataFrame with orderId, evt_time, and chain.
    """
    df = pd.read_csv(file_path)
    processed_df = pd.DataFrame()

    for column in df.columns[1:]:
        df_temp = df[column].apply(lambda x: x.split(", ") if pd.notnull(x) else [])
        df_temp = df_temp.explode()
        df_split = pd.DataFrame()
        df_split[["orderId", "evt_time"]] = df_temp.str.split("-", n=1, expand=True)
        df_split["chain"] = column
        processed_df = pd.concat([processed_df, df_split])

    processed_df.dropna(inplace=True)
    processed_df.set_index("orderId", inplace=True)
    return processed_df


def get_existing_orders(project_id, dataset_id, table_id):
    """
    Fetches existing order IDs from BigQuery to avoid duplicates.
    Args:
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.

    Returns:
        set: Set of existing order IDs in BigQuery.
    """
    client = bigquery.Client.from_service_account_json(GOOGLE_CREDENTIALS_PATH)
    query = f"SELECT DISTINCT orderid FROM `{project_id}.{dataset_id}.{table_id}`"
    query_job = client.query(query)
    existing_orders = {row["orderid"] for row in query_job}
    return existing_orders


def process_and_push_orders(
    src_csv, dest_csv, project_id, dataset_id, table_id, push_new_orders=False
):
    """
    Reads source and destination CSVs, processes the data, checks for duplicates,
    and pushes only new orders to BigQuery.

    Args:
        src_csv (str): Path to source CSV file.
        dest_csv (str): Path to destination CSV file.
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
    """
    df_source = read_and_process_csv(src_csv)
    df_destination = read_and_process_csv(dest_csv)

    dune_orders = pd.concat([df_source, df_destination], axis=1)
    dune_orders.reset_index(inplace=True)
    dune_orders.columns = [
        "orderid",
        "src_evt_time",
        "src_chain",
        "dest_evt_time",
        "dest_chain",
    ]

    print(f"Total orders in CSV: {len(dune_orders)}")

    if push_new_orders:
        print("Checking for existing orders in BigQuery...")
        existing_orders = get_existing_orders(project_id, dataset_id, table_id)
        new_orders = dune_orders[~dune_orders["orderid"].isin(existing_orders)]

        # Push only new orders to BigQuery
        if not new_orders.empty:
            print("Pushing new orders to BigQuery...")
            utilities.load_to_table(
                new_orders, dataset_id, table_id, GOOGLE_CREDENTIALS_PATH
            )
            print("Upload complete!")

            return dune_orders

        else:
            print("No new orders to push.")
    else:
        return dune_orders


# Example Usage
if __name__ == "__main__":
    process_and_push_orders(
        src_csv=os.getenv("src_csv"),
        dest_csv=os.getenv("dest_csv"),
        project_id=os.getenv("project_id"),
        dataset_id=os.getenv("dataset_id"),
        table_id=os.getenv("table_id"),
    )
