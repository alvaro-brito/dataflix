-- Staging: Cleaned ratings
SELECT
    rating_id,
    user_id,
    movie_id,
    rating,
    liked,
    rated_at
FROM raw.ratings
WHERE user_id IS NOT NULL AND movie_id IS NOT NULL
