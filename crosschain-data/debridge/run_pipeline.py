from dune_orders_processor import process_and_push_orders
from dln_orders_fetcher import process_dln_orders
import os
from dotenv import load_dotenv

load_dotenv()

# Step 1: Filter and Push Dune orders to BQ
dune_orders = process_and_push_orders(
    src_csv=os.getenv("src_csv"),
    dest_csv=os.getenv("dest_csv"),
    project_id=os.getenv("project_id"),
    dataset_id=os.getenv("dataset_id"),
    table_id=os.getenv("table_id"),
)

# Step 2: Fetch missing orders from API and push to BQ
process_dln_orders(
    dune_orders=dune_orders,
    project_id=os.getenv("project_id"),
    dataset_id=os.getenv("dataset_id"),
    table_id=os.getenv("table_id_1"),
)
