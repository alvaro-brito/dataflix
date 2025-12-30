-- Mart: User-movie matrix for collaborative filtering
SELECT
    w.user_id as user_id,
    u.username,
    w.movie_id as movie_id,
    m.title,
    CASE WHEN w.watched_id IS NOT NULL THEN 1 ELSE 0 END as watched,
    COALESCE(r.rating, 0) as rating,
    COALESCE(r.liked, 0) as liked,
    -- Combined score: (rating * 0.7) + (liked * 0.3)
    (COALESCE(r.rating, 0) * 0.7) + (COALESCE(r.liked, 0) * 0.3) as interaction_score
FROM {{ ref('stg_watched_movies') }} w
LEFT JOIN {{ ref('stg_ratings') }} r ON w.user_id = r.user_id AND w.movie_id = r.movie_id
LEFT JOIN {{ ref('stg_movies') }} m ON w.movie_id = m.movie_id
LEFT JOIN {{ ref('stg_users') }} u ON w.user_id = u.user_id

UNION ALL

-- Include users who haven't watched but rated
SELECT
    r.user_id as user_id,
    u.username,
    r.movie_id as movie_id,
    m.title,
    0 as watched,
    r.rating,
    r.liked,
    (r.rating * 0.7) + (r.liked * 0.3) as interaction_score
FROM {{ ref('stg_ratings') }} r
LEFT JOIN {{ ref('stg_movies') }} m ON r.movie_id = m.movie_id
LEFT JOIN {{ ref('stg_users') }} u ON r.user_id = u.user_id
WHERE NOT EXISTS (
    SELECT 1 FROM {{ ref('stg_watched_movies') }} w
    WHERE w.user_id = r.user_id AND w.movie_id = r.movie_id
)
