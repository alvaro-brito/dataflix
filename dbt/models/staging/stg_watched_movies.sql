-- Staging: Cleaned watched movies
SELECT
    watched_id,
    user_id,
    movie_id,
    watched_at
FROM raw.watched_movies
WHERE user_id IS NOT NULL AND movie_id IS NOT NULL
