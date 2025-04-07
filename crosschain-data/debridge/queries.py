PAIR_ANALYSIS = """WITH PRICES AS (SELECT 
      ID.ORDERID, 
      ID.SRC_EVT_TIME,

      CASE WHEN LEFT(MakerSRC, 2) != '0x' then 'SOL' 
      WHEN giveOfferWithMetadata_metadata_symbol = 'S' THEN 'SONIC'
      WHEN LEFT(giveOfferWithMetadata_metadata_symbol, 2) = 'sc' THEN 'SONIC'
      ELSE ID.SRC_CHAIN END AS SRC_CHAIN,
      
      CASE WHEN preswapData_tokenInMetadata_name = 'nan'  THEN giveOfferWithMetadata_metadata_symbol 
        ELSE preswapData_tokenInMetadata_name END AS preswapData_tokenInMetadata_name,
    
      giveOfferWithMetadata_metadata_symbol,
      preswapData_tokenOUTMetadata_name,
      takeOfferWithMetadata_metadata_symbol,
      CASE WHEN LEFT(receiverDst, 2)!= '0x' THEN 'SOL' 
      WHEN takeOfferWithMetadata_metadata_symbol = 'S' THEN 'SONIC'
      WHEN LEFT(takeOfferWithMetadata_metadata_symbol, 2) = 'sc' THEN 'SONIC'
      ELSE ID.DEST_CHAIN END AS DEST_CHAIN, 

-- AMOUNT IN 
    CASE WHEN preswapData_tokenInMetadata_name != 'nan' THEN (CAST(preswapData_inAmount AS FLOAT64)
   / POW(10, CAST(preswapData_tokenInMetadata_decimals AS FLOAT64))) 
      ELSE (CAST(giveOfferWithMetadata_finalAmount AS FLOAT64) 
   / POW(10, CAST(giveOfferWithMetadata_metadata_decimals AS FLOAT64))) 
      END AS AMOUNT_IN,

-- AMOUNT OUT
    (CAST(takeOfferWithMetadata_finalAmount AS FLOAT64) 
   / POW(10, CAST(takeOfferWithMetadata_metadata_decimals AS FLOAT64))) AS AMOUNT_OUT,

-- FINAL PRICE
   (CAST(takeOfferWithMetadata_finalAmount AS FLOAT64) 
   / POW(10, CAST(takeOfferWithMetadata_metadata_decimals AS FLOAT64))) 
   / CASE WHEN preswapData_tokenInMetadata_name != 'nan' THEN (CAST(preswapData_inAmount AS FLOAT64)
   / POW(10, CAST(preswapData_tokenInMetadata_decimals AS FLOAT64))) 
      ELSE (CAST(giveOfferWithMetadata_finalAmount AS FLOAT64) 
   / POW(10, CAST(giveOfferWithMetadata_metadata_decimals AS FLOAT64))) 
      END AS FINAL_PRICE,

-- PAIR
   CONCAT(
    CASE WHEN preswapData_tokenInMetadata_name = 'nan' THEN giveOfferWithMetadata_metadata_symbol 
        ELSE preswapData_tokenInMetadata_name END, "-", takeOfferWithMetadata_metadata_symbol) AS PAIR
  
  FROM `silken-mile-379810.crosschain_dataset.debridge-data` DATA
  LEFT JOIN (
    SELECT * FROM `silken-mile-379810.crosschain_dataset.debridge-dune-data`) ID
    ON ID.ORDERID = DATA.ORDERID
  WHERE CAST(giveOfferWithMetadata_finalAmount AS FLOAT64) > 0)


SELECT 
    PAIR, 
    COUNT(PAIR) AS TX_COUNT, 
    AVG(FINAL_PRICE) AS AVG_PRICE,
    SUM(AMOUNT_IN) AS AMOUNT_IN,
    SUM(AMOUNT_OUT) AS AMOUNT_OUT, 
    SUM(
          CASE
        WHEN split(pair, '-')[1] LIKE '%SOL%' THEN 150
        WHEN split(pair, '-')[1] LIKE '%ETH%' THEN 2400
        WHEN split(pair, '-')[1] LIKE '%Eth%' THEN 2400
        WHEN split(pair, '-')[1] LIKE 'BNB' THEN 500
        WHEN split(pair, '-')[1] LIKE '%USD%' THEN 1
        ELSE 0 END * AMOUNT_OUT
    ) AS VOLUME_USD
FROM PRICES
GROUP BY PAIR"
"""

CHAIN_ANALYSIS = """WITH PRICES AS (SELECT 
      ID.ORDERID, 
      ID.SRC_EVT_TIME,
      CASE WHEN preswapData_tokenInMetadata_name = 'nan'  THEN giveOfferWithMetadata_metadata_symbol 
        ELSE preswapData_tokenInMetadata_name END AS preswapData_tokenInMetadata_name,
      giveOfferWithMetadata_metadata_symbol,
      preswapData_tokenOUTMetadata_name,
      takeOfferWithMetadata_metadata_symbol,
  
-- SRC & DEST CHAINS
      CASE WHEN LEFT(MakerSRC, 2) != '0x' then 'SOL' 
      WHEN giveOfferWithMetadata_metadata_symbol = 'S' THEN 'SONIC'
      WHEN LEFT(giveOfferWithMetadata_metadata_symbol, 2) = 'sc' THEN 'SONIC'
      ELSE ID.SRC_CHAIN END AS SRC_CHAIN,

      CASE WHEN LEFT(receiverDst, 2)!= '0x' THEN 'SOL' 
      WHEN takeOfferWithMetadata_metadata_symbol = 'S' THEN 'SONIC'
      WHEN LEFT(takeOfferWithMetadata_metadata_symbol, 2) = 'sc' THEN 'SONIC'
      ELSE ID.DEST_CHAIN END AS DEST_CHAIN, 

-- AMOUNT IN 
    CASE WHEN preswapData_tokenInMetadata_name != 'nan' THEN (CAST(preswapData_inAmount AS FLOAT64)
   / POW(10, CAST(preswapData_tokenInMetadata_decimals AS FLOAT64))) 
      ELSE (CAST(giveOfferWithMetadata_finalAmount AS FLOAT64) 
   / POW(10, CAST(giveOfferWithMetadata_metadata_decimals AS FLOAT64))) 
      END AS AMOUNT_IN,

-- AMOUNT OUT
    (CAST(takeOfferWithMetadata_finalAmount AS FLOAT64) 
   / POW(10, CAST(takeOfferWithMetadata_metadata_decimals AS FLOAT64))) AS AMOUNT_OUT,

-- FINAL PRICE
   (CAST(takeOfferWithMetadata_finalAmount AS FLOAT64) 
   / POW(10, CAST(takeOfferWithMetadata_metadata_decimals AS FLOAT64))) 
   / CASE WHEN preswapData_tokenInMetadata_name != 'nan' THEN (CAST(preswapData_inAmount AS FLOAT64)
   / POW(10, CAST(preswapData_tokenInMetadata_decimals AS FLOAT64))) 
      ELSE (CAST(giveOfferWithMetadata_finalAmount AS FLOAT64) 
   / POW(10, CAST(giveOfferWithMetadata_metadata_decimals AS FLOAT64))) 
      END AS FINAL_PRICE,

-- PAIR
   CONCAT(
    CASE WHEN preswapData_tokenInMetadata_name = 'nan' THEN giveOfferWithMetadata_metadata_symbol 
        ELSE preswapData_tokenInMetadata_name END, "-", takeOfferWithMetadata_metadata_symbol) AS PAIR
  
  FROM `silken-mile-379810.crosschain_dataset.debridge-data` DATA
  LEFT JOIN (
    SELECT * FROM `silken-mile-379810.crosschain_dataset.debridge-dune-data`) ID
    ON ID.ORDERID = DATA.ORDERID
  WHERE CAST(giveOfferWithMetadata_finalAmount AS FLOAT64) > 0)


SELECT 
    CONCAT(SRC_CHAIN, "-", DEST_CHAIN) AS CHAIN, 
    COUNT(CONCAT(SRC_CHAIN, "-", DEST_CHAIN)) AS TX_COUNT, 
    AVG(FINAL_PRICE) AS AVG_PRICE,
    SUM(AMOUNT_IN) AS AMOUNT_IN,
    SUM(AMOUNT_OUT) AS AMOUNT_OUT, 
    SUM(
          CASE
        WHEN split(pair, '-')[1] LIKE '%SOL%' THEN 150
        WHEN split(pair, '-')[1] LIKE '%ETH%' THEN 2400
        WHEN split(pair, '-')[1] LIKE '%Eth%' THEN 2400
        WHEN split(pair, '-')[1] LIKE 'BNB' THEN 500
        WHEN split(pair, '-')[1] LIKE '%USD%' THEN 1
        ELSE 0 END * AMOUNT_OUT
    ) AS VOLUME_USD
FROM PRICES
GROUP BY CHAIN
"""
