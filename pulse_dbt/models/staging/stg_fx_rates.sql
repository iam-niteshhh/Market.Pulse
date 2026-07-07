-- Thin wrapper on Spark Silver fx_rates table.
-- Renames, casts, adds surrogate key. No business logic.

with source as (
    select * from {{source("spark_silver", "fx_rates")}}
),
renamed as (
    select
    -- keys
    {{dbt_utils.generate_surrogate_key(['base', 'target', 'timestamp'])}}  as fx_rate_id,

    -- dimensions
        CAST(base  AS STRING)  AS base_currency,
        CAST(target AS STRING)  AS target_currency,
        CAST(rate           AS DOUBLE)  AS rate,
        CAST(timestamp      AS TIMESTAMP)  AS timestamp,

        current_timestamp() as dbt_loaded_at

    FROM source
    WHERE base IS NOT NULL AND target IS NOT NULL
)
SELECT * FROM renamed