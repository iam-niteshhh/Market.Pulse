-- Thin wrapper on Spark Gold stock_prices table.
-- Renames, casts, adds surrogate key. No business logic.

with source as (
    select * from {{source("../../data/delta/gold", "stock_prices")}}
),
renamed as (
    select
    -- keys
    {{dbt_utils.generate_surrogate_key(['ticker', 'window_start'])}}  as stock_id
    
    -- dimensions
        CAST(ticker         AS STRING)  AS ticker,
        -- time window
        CAST(window_start   AS TIMESTAMP) AS window_start,
        CAST(window_end     AS TIMESTAMP) AS window_end,
        -- measures
        CAST(avg_open       AS DOUBLE)  AS avg_open,
        CAST(avg_high       AS DOUBLE)  AS avg_high,
        CAST(avg_low        AS DOUBLE)  AS avg_low,
        CAST(avg_close      AS DOUBLE)  AS avg_close,
        CAST(avg_volume     AS BIGINT)  AS avg_volume,
        -- metadata
        CAST(record_count   AS INT)     AS record_count,
        CURRENT_TIMESTAMP()             AS dbt_loaded_at

    FROM source
    WHERE ticker IS NOT NULL
      AND avg_close > 0

)

SELECT * FROM renamed