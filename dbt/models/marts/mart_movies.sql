-- Mart: Movies with aggregations
SELECT
    m.movie_id,
    m.title,
    m.genre,
    m.release_year,
    m.director,
    m.duration_minutes,
    m.imdb_rating,
    COUNT(DISTINCT w.user_id) as total_watches,
    COUNT(DISTINCT r.user_id) as total_ratings,
    AVG(r.rating) as average_user_rating,
    m.created_at
FROM {{ ref('stg_movies') }} m
LEFT JOIN {{ ref('stg_watched_movies') }} w ON m.movie_id = w.movie_id
LEFT JOIN {{ ref('stg_ratings') }} r ON m.movie_id = r.movie_id
GROUP BY
    m.movie_id,
    m.title,
    m.genre,
    m.release_year,
    m.director,
    m.duration_minutes,
    m.imdb_rating,
    m.created_at
