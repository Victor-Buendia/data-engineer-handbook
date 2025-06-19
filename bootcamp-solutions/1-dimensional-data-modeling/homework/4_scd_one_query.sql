-- INSERT INTO actors_history_scd
WITH changed_data AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        case
            when lag(quality_class, 1) over (partition by actorid order by year) <> quality_class then 1
            when lag(is_active, 1) over (partition by actorid order by year) <> is_active then 1
            else 0
        end as changed_data,
        year
    FROM actors
),
streaks AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        sum(changed_data) over(partition by actorid order by year) as streak,
        year
    FROM changed_data
),
scd_history_actors AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        streak,
        MIN(year) AS start_date,
        MAX(year) AS end_date
    FROM streaks
    GROUP BY actorid, actor, quality_class, is_active, streak
    ORDER BY actorid, start_date
)

SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
FROM scd_history_actors
-- WHERE actorid = 'nm0000057'