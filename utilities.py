from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from sqlalchemy import create_engine


def load_to_table(df, dataset_id, table_id, credentials_path):
    """
    Uploads a pandas DataFrame to a Google BigQuery table.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        table_id = f"{client.project}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig()

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print(f"Data successfully loaded to table '{table_id}'.")

    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")
        print(f"Failed to add data to table '{table_id}'.")


def get_BQ(query, credentials_path):
    """
    Fetches data from BigQuery using a SQL query.
    Parameters: query (str): The SQL query to execute.
    Returns: pd.DataFrame: The resulting DataFrame.
    """
    try:
        # Create a BigQuery engine using SQLAlchemy
        bq_engine = create_engine("bigquery://", credentials_path=credentials_path)
        df = pd.read_sql(query, bq_engine)
        return df
    except Exception as e:
        print(f"Error fetching data from BigQuery: {e}")
        return pd.DataFrame()
