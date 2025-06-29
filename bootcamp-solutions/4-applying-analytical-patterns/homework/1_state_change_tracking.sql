-- CREATE TABLE player_state_tracking (
--     dim_player_name TEXT,
--     dim_first_season INT,
--     dim_last_season INT,
--     dim_season_activity_state TEXT,
--     dim_season INT,
--     PRIMARY KEY (dim_player_name, dim_season)
-- );

-- DELETE FROM player_state_tracking;

insert into player_state_tracking
with current_season_var as (
    select 2004 as curr_season
),
yesterday as (
    select
        dim_player_name,
        dim_first_season,
        dim_last_season,
        dim_season_activity_state,
        dim_season
    from player_state_tracking
    where dim_season = (select (curr_season - 1) from current_season_var)
),
today as (
    select
        player_name,
        season
    from player_seasons
    where season = (select curr_season from current_season_var)
    group by player_name, season
),
state_tracking as (
    select
        coalesce(y.dim_player_name, t.player_name) as dim_player_name,
        coalesce(y.dim_first_season, t.season) as dim_first_season,
        coalesce(t.season, y.dim_last_season) as dim_last_season,
        case
            when y.dim_player_name is null and t.player_name is not null then 'New'
            when y.dim_player_name is not null and t.player_name is null and (y.dim_last_season + 1) = (y.dim_season + 1) then 'Retired'
            when y.dim_player_name is not null and t.player_name is null and (y.dim_last_season + 1) < (y.dim_season + 1) then 'Stayed Retired'
            when (y.dim_last_season + 1) = t.season then 'Continued Playing'
            when (y.dim_last_season + 1) < (t.season) then 'Returned from Retirement'
        end as dim_season_activity_state,
        coalesce(t.season, y.dim_season + 1) as dim_season
    from today t
    full outer join yesterday y
        on t.player_name = y.dim_player_name
)

select * from state_tracking