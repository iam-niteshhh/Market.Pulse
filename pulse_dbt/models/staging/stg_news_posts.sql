-- Thin wrapper on Spark Silver news_posts table.
-- Renames, casts, adds surrogate key. No business logic.
WITH source AS (
    SELECT *
    FROM delta.`/data/delta/silver/news_posts`
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id', 'published_at']) }} AS news_id,

        CAST(id AS STRING) AS id,
        CAST(title AS STRING) AS title,
        CAST(description AS STRING) AS description,
        CAST(content AS STRING) AS content,
        CAST(author AS STRING) AS author,
        CAST(source AS STRING) AS source,
        CAST(url AS STRING) AS url,
        CAST(image_url AS STRING) AS url_to_image,
        CAST(published_at AS TIMESTAMP) AS published_at,

        CAST(sentiment_polarity AS DOUBLE) AS sentiment_score,
        CAST(sentiment_subjectivity AS DOUBLE) AS sentiment_subjectivity,

        current_timestamp() AS dbt_loaded_at

    FROM source
    WHERE id IS NOT NULL
)

SELECT *
FROM renamed