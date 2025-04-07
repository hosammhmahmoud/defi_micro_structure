DeBridge Orders - Intent-based Bridging

Overview
This repository provides a streamlined process to track and process DLN orders across multiple chains. The workflow integrates Dune Analytics, BigQuery, and the DLN API to maintain a complete and up-to-date list of order IDs.

Workflow
The following steps outline how to fetch, process, and store order data:

1️⃣ Extract Data from Dune Analytics
- Use Dune Analytics to retrieve order data (orderId, evt_time, chain) for both SRC and DEST events.
- Export the data as a CSV file.
2️⃣ Process Orders & Merge Data
- Run the script: dune_order_processing.py
- This merges the source (SRC) and destination (DEST) order data.
- The processed data is pushed to BigQuery to maintain a complete list of processed order IDs.
3️⃣ Identify Unprocessed Orders
- Compare the processed orders in BigQuery with dln_data_v2 to filter out unprocessed orderIds.
- This ensures that only new orders are fetched and processed.
4️⃣ Fetch & Push Orders to BigQuery
- Fetch new orders from the DLN API.
- Process the data and push it to BigQuery for further analysis.


Dune Analytics Query
Use the following SQL query to extract order data from Dune Analytics:

SELECT 
    date_trunc('hour', EVT_BLOCK_TIME) AS event_hour,
    coalesce(array_join(array_agg(concat(cast(ORDERID AS varchar), '-', cast(EVT_BLOCK_TIME AS varchar)))
    FILTER (WHERE CHAIN = 'ethereum'), ', '), '') AS ETH,
    coalesce(array_join(array_agg(concat(cast(ORDERID AS varchar), '-', cast(EVT_BLOCK_TIME AS varchar)))
    FILTER (WHERE CHAIN = 'bnb'), ', '), '') AS BSC,
    coalesce(array_join(array_agg(concat(cast(ORDERID AS varchar), '-', cast(EVT_BLOCK_TIME AS varchar)))
    FILTER (WHERE CHAIN = 'arbitrum'), ', '), '') AS ARB,
    coalesce(array_join(array_agg(concat(cast(ORDERID AS varchar), '-', cast(EVT_BLOCK_TIME AS varchar)))
    FILTER (WHERE CHAIN = 'avalanche_c'), ', '), '') AS AVAX,
    coalesce(array_join(array_agg(concat(cast(ORDERID AS varchar), '-', cast(EVT_BLOCK_TIME AS varchar)))
    FILTER (WHERE CHAIN = 'polygon'), ', '), '') AS POL,
    coalesce(array_join(array_agg(concat(cast(ORDERID AS varchar), '-', cast(EVT_BLOCK_TIME AS varchar)))
    FILTER (WHERE CHAIN = 'linea'), ', '), '') AS LIN,
    coalesce(array_join(array_agg(concat(cast(ORDERID AS varchar), '-', cast(EVT_BLOCK_TIME AS varchar)))
    FILTER (WHERE CHAIN = 'base'), ', '), '') AS BASE,
    coalesce(array_join(array_agg(concat(cast(ORDERID AS varchar), '-', cast(EVT_BLOCK_TIME AS varchar)))
    FILTER (WHERE CHAIN = 'optimism'), ', '), '') AS OP
FROM dln_trade_multichain.dlnsource_evt_createdorder
WHERE EVT_BLOCK_TIME >= TIMESTAMP '2025-02-01 00:00:00'
  AND EVT_BLOCK_TIME < TIMESTAMP '2025-03-01 00:00:00'
GROUP BY date_trunc('hour', EVT_BLOCK_TIME)
ORDER BY event_hour;