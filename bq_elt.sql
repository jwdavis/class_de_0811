SELECT
    transaction_id,
    -- Attempt to cast to NUMERIC. If it fails, SAFE_CAST returns NULL.
    SAFE_CAST(sale_amount AS NUMERIC) AS clean_sale_amount,
    customer_email,
    order_date,
    product_code,
    quantity,
    status
FROM
    my_dataset.raw_data_staging;

SELECT
    transaction_id,
    sale_amount,
    customer_email,
    -- Attempt to parse common date formats.
    -- Prioritize the most common/desired format.
    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', order_date), -- '2023-01-15'
        SAFE.PARSE_DATE('%m/%d/%Y', order_date), -- '01/15/2023'
        SAFE.PARSE_DATE('%d-%b-%Y', order_date)  -- '15-Jan-2023'
    ) AS clean_order_date,
    product_code,
    quantity,
    status
FROM
    my_dataset.raw_data_staging;

SELECT
    transaction_id,
    sale_amount,
    -- Trim whitespace and convert empty strings to NULL
    NULLIF(TRIM(customer_email), '') AS clean_customer_email,
    order_date,
    -- Trim whitespace and convert empty strings to NULL
    NULLIF(TRIM(product_code), '') AS clean_product_code,
    quantity,
    status
FROM
    my_dataset.raw_data_staging;

SELECT
    transaction_id,
    sale_amount,
    customer_email,
    order_date,
    product_code,
    quantity,
    -- Validate and standardize status values
    CASE
        WHEN status IN ('Completed', 'Pending', 'Cancelled') THEN status
        WHEN status = 'Done' THEN 'Completed' -- Standardize 'Done' to 'Completed'
        ELSE 'Unknown' -- Or NULL, depending on your requirement
    END AS clean_status
FROM
    my_dataset.raw_data_staging;


SELECT
    transaction_id,
    SAFE_CAST(sale_amount AS NUMERIC) AS clean_sale_amount,
    NULLIF(TRIM(customer_email), '') AS clean_customer_email,
    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', order_date),
        SAFE.PARSE_DATE('%m/%d/%Y', order_date)
    ) AS clean_order_date,
    NULLIF(TRIM(product_code), '') AS clean_product_code,
    SAFE_CAST(quantity AS INT64) AS clean_quantity,
    CASE
        WHEN status IN ('Completed', 'Pending', 'Cancelled') THEN status
        WHEN status = 'Done' THEN 'Completed'
        ELSE 'Unknown'
    END AS clean_status
FROM
    my_dataset.raw_data_staging
-- Filter out rows where critical columns are NULL after cleaning attempts
WHERE
    transaction_id IS NOT NULL
    AND SAFE_CAST(sale_amount AS NUMERIC) IS NOT NULL -- Ensures sale_amount is valid and not NULL
    AND SAFE_CAST(quantity AS INT64) >= 1 -- Quantity must be at least 1
QUALIFY
    -- Deduplicate based on transaction_id, keeping the first encountered record
    ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY order_date DESC) = 1;
    -- The ORDER BY in ROW_NUMBER() determines which duplicate to keep.
    -- Here, we prioritize the most recent order_date if duplicates exist.