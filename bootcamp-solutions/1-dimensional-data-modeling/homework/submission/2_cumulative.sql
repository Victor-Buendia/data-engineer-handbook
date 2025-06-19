INSERT INTO
    actors WITH yesterday AS (
        SELECT
            actorid,
            actor,
            films,
            quality_class,
            is_active,
            year
        FROM
            actors
        WHERE
            year = 1969
    ),
    today AS (
        SELECT
            actorid,
            actor,
            array_agg(
                ROW (
                    film,
                    votes,
                    year,
                    rating,
                    filmid
                ) :: films
            ) AS films,
            CASE
                WHEN AVG(rating) > 8 THEN 'star'
                WHEN AVG(rating) > 7 THEN 'good'
                WHEN AVG(rating) > 6 THEN 'average'
                ELSE 'bad'
            END :: quality_class AS quality_class,
            year
        FROM
            actor_films
        WHERE
            year = 1970
        GROUP BY
            actorid,
            actor,
            year
    ),
    actors_data AS (
        SELECT
            COALESCE(t.actorid, y.actorid) AS actorid,
            COALESCE(t.actor, y.actor) AS actor,
            CASE
                WHEN t.actor IS NULL THEN y.films
                ELSE ARRAY [] :: films [] || t.films
            END AS films,
            COALESCE(t.quality_class, y.quality_class) AS quality_class,
            t.actor IS NOT NULL as is_active,
            COALESCE(t.year, y.year + 1) AS year
        FROM
            yesterday AS y
            FULL OUTER JOIN today AS t ON y.actorid = t.actorid
    )
SELECT
    *
FROM
    actors_data;