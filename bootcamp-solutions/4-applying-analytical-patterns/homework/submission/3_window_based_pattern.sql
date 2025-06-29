with game_dets as (
    select
        gd.player_id,
        gd.player_name,
        gd.team_id,
        gd.team_abbreviation,
        gd.game_id,
        g.season,
        g.game_date_est,
        (
            (g.home_team_id = gd.team_id and g.home_team_wins = 1)
            or (g.home_team_id <> gd.team_id and g.home_team_wins = 0)
        ) as team_id_won_game,
        coalesce(gd.pts, 0) as pts
    from game_details gd
    join games g
        on gd.game_id = g.game_id
),
games_ as (
    select
        game_id,
        team_id,
        team_abbreviation,
        team_id_won_game,
        season,
        game_date_est,
        row_number() over (partition by game_id) as rn
    from game_dets
    order by game_date_est
),
deduped as (
    select * from games_ where rn = 1
),
stretch as (
    select
    game_id,
    team_id,
    team_abbreviation,
    team_id_won_game,
    season,
    game_date_est,
    (sum(case when team_id_won_game then 1 else 0 end)
    over(partition by team_id order by game_date_est rows between 89 preceding and current row)) as stretch_wins_90_days
--     row_number() over(partition by team_id order by game_date_est) as rn
from deduped
)

select
    team_id,
    team_abbreviation,
    max(stretch_wins_90_days) as max_wins_stretch_90_days
from stretch
group by team_id, team_abbreviation
order by max_wins_stretch_90_days desc