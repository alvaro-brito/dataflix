-- Mart: Aggregated ratings by movie
SELECT
    r.movie_id,
    m.title,
    COUNT(*) as total_ratings,
    AVG(r.rating) as average_rating,
    SUM(r.liked) as total_likes,
    (SUM(r.liked) * 100.0 / COUNT(*)) as like_percentage,
    NOW() as last_updated
FROM {{ ref('stg_ratings') }} r
LEFT JOIN {{ ref('stg_movies') }} m ON r.movie_id = m.movie_id
GROUP BY r.movie_id, m.title
