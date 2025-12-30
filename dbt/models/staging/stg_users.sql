-- Staging: Cleaned and prepared users
SELECT
    user_id,
    username,
    city,
    state,
    country,
    age,
    created_at
FROM raw.users
WHERE user_id IS NOT NULL
