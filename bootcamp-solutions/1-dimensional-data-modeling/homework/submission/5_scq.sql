CREATE TYPE actor_scd AS (
    quality_class quality_class,
    is_active bool,
    start_year int,
    end_year int
);

INSERT INTO actors_history_scd
WITH historical_data AS (
    SELECT * FROM actors_history_scd
    WHERE year = 1973 AND end_date < 1973
),
last_year_data AS (
    SELECT *
    FROM actors_history_scd
    WHERE year = 1973 AND end_date = 1973
),
this_year_data AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        year
    FROM actors
    WHERE year = 1974
),
changed_records AS (
    SELECT
        ly.actorid,
        ly.actor,
        (unnest( -- turn columns into rows
            array[
                row(ly.quality_class, ly.is_active, ly.start_date, ly.end_date)::actor_scd,
                row(ty.quality_class, ty.is_active, ty.year, ty.year)::actor_scd
            ]
        )::actor_scd).* -- turn struct into columns
    FROM last_year_data ly
    JOIN this_year_data ty
        ON ly.actorid = ty.actorid
        AND (ly.quality_class <> ty.quality_class OR ly.is_active <> ty.is_active)
),
unchanged_records AS (
    SELECT
        ly.actorid,
        ly.actor,
        ly.quality_class,
        ly.is_active,
        ly.start_date,
        ty.year as end_date
    FROM last_year_data ly
    JOIN this_year_data ty
        ON ly.actorid = ty.actorid
        AND ly.quality_class = ty.quality_class
        AND ly.is_active = ty.is_active
),
new_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ty.year AS start_date,
        ty.year AS end_date
    FROM this_year_data ty
    LEFT JOIN last_year_data ly
        ON ty.actorid = ly.actorid
    WHERE ly.actorid IS NULL
),
final_data AS (
    SELECT * FROM new_records
    UNION
    SELECT * from changed_records
    UNION
    SELECT * from unchanged_records
)

SELECT *, 1974 as year
FROM final_data