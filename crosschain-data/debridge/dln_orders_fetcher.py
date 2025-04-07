import os
import requests
import pandas as pd
from tqdm import tqdm
import time
import utilities
from google.cloud import bigquery
from dune_orders_processor import get_existing_orders
from dotenv import load_dotenv

load_dotenv()

GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")

# API Endpoint
BASE_URL = "https://stats-api.dln.trade/api/Orders/"
BATCH_SIZE = 1000


def get_dln_order(order_id, max_retries=5):
    """
    Fetch an order from deBridge API by order ID with retry logic.

    Args:
        order_id (str): The order ID to fetch.
        max_retries (int): Maximum retry attempts.

    Returns:
        dict or None: API response in JSON format or None if failed.
    """
    attempt, wait_time = 0, 1
    while attempt < max_retries:
        try:
            response = requests.get(BASE_URL + order_id, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"Rate limit exceeded (429). Retrying in {wait_time} seconds...")
            elif response.status_code >= 500:
                print(
                    f"Server error ({response.status_code}). Retrying in {wait_time} seconds..."
                )
            else:
                print(
                    f"Error fetching {order_id}: {response.status_code} - {response.text}"
                )
                break
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}. Retrying in {wait_time} seconds...")

        time.sleep(wait_time)
        wait_time *= 2
        attempt += 1
    return None


def flatten(d, parent_key="", sep="."):
    """
    Recursively flattens a nested dictionary, extracting only 'stringValue' if available.

    Args:
        d (dict): Input dictionary.
        parent_key (str): Parent key prefix.
        sep (str): Separator for nested keys.

    Returns:
        dict: Flattened dictionary.
    """
    items = {}
    for key, value in d.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            if "stringValue" in value:
                items[new_key] = value["stringValue"]
            else:
                items.update(flatten(value, new_key, sep=sep))
        else:
            items[new_key] = str(value) if not isinstance(value, (str, int)) else value
    return items


def upload_batch(df, batch_counter, dataset_id, table_id):
    """
    Saves the DataFrame to a CSV, uploads it to BigQuery, and deletes the file.

    Args:
        df (pd.DataFrame): DataFrame containing the batch of orders.
        batch_counter (int): The batch number.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
    """
    df.columns = df.columns.str.replace(".", "_", regex=False)

    df = df.astype(str)

    if "orderMetadata_rawMetadataHex" in df.columns:
        df.drop("orderMetadata_rawMetadataHex", axis=1, inplace=True)

    # Upload to BigQuery
    utilities.load_to_table(df, dataset_id, table_id, GOOGLE_CREDENTIALS_PATH)
    print(f"Uploaded batch {batch_counter} to BigQuery.")


def process_dln_orders(dune_orders, project_id, dataset_id, table_id):
    """
    Reads a CSV of order IDs, filters out existing orders in BQ, fetches new ones,
    processes them, and uploads in 10k batches.

    Args:
        csv_path (str): Path to input CSV containing order IDs.
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
    """
    print("Fetching existing order IDs from BigQuery...")
    existing_orders = get_existing_orders(project_id, dataset_id, table_id)

    # Filter out already processed orders
    print(f"Total orders in CSV: {len(dune_orders)}")
    print(dune_orders)
    dune_orders = dune_orders[~dune_orders["orderid"].isin(existing_orders)]
    print(f"Orders after filtering existing ones: {len(dune_orders)}")

    df_final = pd.DataFrame()
    batch_counter = 1

    for i, row in tqdm(dune_orders.iterrows(), total=len(dune_orders)):
        try:
            order_id = row["orderid"]
            order_response = get_dln_order(order_id)
            if order_response:
                flat_order = flatten(order_response)
                df_order = pd.DataFrame([flat_order])
                df_final = pd.concat([df_final, df_order], ignore_index=True)

            if len(df_final) >= BATCH_SIZE:
                upload_batch(df_final, batch_counter, dataset_id, table_id)
                batch_counter += 1
                df_final = pd.DataFrame()  # Reset for next batch

        except Exception as e:
            print(f"Error processing orderId {row.get('orderId', i)}: {e}")
            time.sleep(10)

    if not df_final.empty:
        upload_batch(df_final, batch_counter, dataset_id, table_id)


# Example Usage
if __name__ == "__main__":
    process_dln_orders(
        dune_orders="dune_orders.csv",
        project_id="silken-mile-379810",
        dataset_id="crosschain_dataset",
        table_id="debridge-data",
    )
