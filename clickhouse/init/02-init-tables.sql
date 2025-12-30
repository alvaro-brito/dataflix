-- ============================================================================
-- Dataflix - ClickHouse Raw Tables
-- ============================================================================

-- Raw Users (with sensitive data masking)
CREATE TABLE IF NOT EXISTS raw.users (
    user_id Int32,
    username String,
    email_hash String,  -- Masked Email (hash)
    first_name_masked String,  -- Masked First Name
    last_name_masked String,  -- Masked Last Name
    city String,
    state String,
    country String,
    age Int32,
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY user_id;

-- Raw Movies
CREATE TABLE IF NOT EXISTS raw.movies (
    movie_id Int32,
    title String,
    description String,
    genre String,
    release_year Int32,
    director String,
    duration_minutes Int32,
    imdb_rating Float64,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY movie_id;

-- Raw Watched Movies
CREATE TABLE IF NOT EXISTS raw.watched_movies (
    watched_id Int32,
    user_id Int32,
    movie_id Int32,
    watched_at DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, movie_id);

-- Raw Ratings
CREATE TABLE IF NOT EXISTS raw.ratings (
    rating_id Int32,
    user_id Int32,
    movie_id Int32,
    rating Float64,
    liked UInt8,
    rated_at DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, movie_id);

-- ============================================================================
-- Dataflix - ClickHouse Analytics Tables (created by dbt)
-- ============================================================================

-- Analytics Users (cleaned and prepared data)
CREATE TABLE IF NOT EXISTS analytics.users (
    user_id Int32,
    username String,
    city String,
    state String,
    country String,
    age Int32,
    total_movies_watched Int32,
    total_ratings Int32,
    average_rating Float64,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY user_id;

-- Analytics Movies
CREATE TABLE IF NOT EXISTS analytics.movies (
    movie_id Int32,
    title String,
    genre String,
    release_year Int32,
    director String,
    duration_minutes Int32,
    imdb_rating Float64,
    total_watches Int32,
    total_ratings Int32,
    average_user_rating Float64,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY movie_id;

-- Analytics User-Movie Matrix (for collaborative filtering)
CREATE TABLE IF NOT EXISTS analytics.user_movie_matrix (
    user_id Int32,
    movie_id Int32,
    watched UInt8,
    rating Float64,
    liked UInt8,
    interaction_score Float64  -- Combined score for recommendation
) ENGINE = MergeTree()
ORDER BY (user_id, movie_id);

-- Analytics Ratings Aggregated
CREATE TABLE IF NOT EXISTS analytics.ratings_aggregated (
    movie_id Int32,
    total_ratings Int32,
    average_rating Float64,
    total_likes Int32,
    like_percentage Float64,
    last_updated DateTime
) ENGINE = MergeTree()
ORDER BY movie_id;
