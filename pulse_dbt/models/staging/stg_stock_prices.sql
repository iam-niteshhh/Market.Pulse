-- Thin wrapper on Spark Gold stock_prices table.
-- Renames, casts, adds surrogate key. No business logic.

with source as (
    select * from {{source("spark_silver", "stock_prices")}}
),
renamed as (
    select
    -- keys
    {{dbt_utils.generate_surrogate_key(['ticker', 'timestamp'])}}  as stock_id,
    
    -- dimensions
        CAST(ticker         AS STRING)  AS ticker,
        
        CAST(open           AS DOUBLE)  AS open,
        CAST(high           AS DOUBLE)  AS high,
        CAST(low            AS DOUBLE)  AS low,
        CAST(close          AS DOUBLE)  AS close,
        CAST(volume         AS DOUBLE)  AS volume,
        CAST(timestamp       AS TIMESTAMP)  AS timestamp,

        current_timestamp() as dbt_loaded_at

    FROM source
    WHERE ticker IS NOT NULL

)

SELECT * FROM renamed