-- Mart: Users with aggregations
SELECT
    u.user_id,
    u.username,
    u.city,
    u.state,
    u.country,
    u.age,
    COUNT(DISTINCT w.movie_id) as total_movies_watched,
    COUNT(DISTINCT r.movie_id) as total_ratings,
    AVG(r.rating) as average_rating,
    u.created_at
FROM {{ ref('stg_users') }} u
LEFT JOIN {{ ref('stg_watched_movies') }} w ON u.user_id = w.user_id
LEFT JOIN {{ ref('stg_ratings') }} r ON u.user_id = r.user_id
GROUP BY
    u.user_id,
    u.username,
    u.city,
    u.state,
    u.country,
    u.age,
    u.created_at
