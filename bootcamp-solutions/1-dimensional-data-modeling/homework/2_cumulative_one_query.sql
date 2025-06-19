INSERT INTO actors
WITH years AS (
    SELECT
        generate_series(1969, 2021) AS year
),
actors_first_year AS (
    SELECT
        actorid,
        MIN(year) AS first_year
    FROM actor_films
    GROUP BY actorid
),
actors_years AS (
    SELECT
        afy.actorid,
        y.year
    FROM actors_first_year afy
    JOIN years y
        ON y.year >= afy.first_year
),
fixed_dims AS (
    SELECT
        actorid,
        actor
    FROM actor_films
    GROUP BY actorid, actor
),
cumulated_actors AS (
    SELECT
        ay.actorid,
        array_remove(
            array_agg(
                case
                    when af.actorid is not null then row(
                    af.film,
                    af.votes,
                    af.year,
                    af.rating,
                    af.filmid
                    )::films
                    end
            ) over(partition by ay.actorid order by ay.year),
            null
        ) as films,
        avg(af.rating) over(partition by ay.actorid order by ay.year),
        sum(af.rating) over(partition by ay.actorid order by ay.year),
        case
            when avg(af.rating) over(partition by ay.actorid order by ay.year) > 8 then 'star'
            when avg(af.rating) over(partition by ay.actorid order by ay.year) > 7 then 'good'
            when avg(af.rating) over(partition by ay.actorid order by ay.year) > 6 then 'average'
            else 'bad'
        end::quality_class as quality_class,
        af.actorid is not null as is_active,
        row_number() over (partition by ay.actorid, ay.year) as rn,
        ay.year
    FROM actors_years ay
    LEFT JOIN actor_films af
        ON af.actorid = ay.actorid
        AND af.year = ay.year
    ORDER BY ay.actorid, ay.year
),
final_cumulated_table AS (
    SELECT
        ca.actorid,
        fd.actor,
        ca.films,
        ca.quality_class,
        ca.is_active,
        ca.year
    FROM cumulated_actors ca
    JOIN fixed_dims fd
        ON ca.actorid = fd.actorid
    WHERE rn = 1
)

SELECT *
-- , array_length(films, 1) as films_made
FROM final_cumulated_table
-- WHERE actorid = 'nm0000057'