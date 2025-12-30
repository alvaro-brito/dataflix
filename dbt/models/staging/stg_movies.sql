-- Staging: Cleaned and prepared movies
SELECT
    movie_id,
    title,
    description,
    genre,
    release_year,
    director,
    duration_minutes,
    imdb_rating,
    created_at
FROM raw.movies
WHERE movie_id IS NOT NULL
