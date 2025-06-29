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
scores as (
    select
        game_id,
        player_id,
        player_name,
        pts,
        game_date_est,
        case when pts > 10 then 1 else 0 end as scored_more_10_pts
    from game_dets
    where player_name = 'LeBron James'
    order by game_date_est
),
streaks as (
    select
        game_id,
        player_id,
        player_name,
        pts,
        game_date_est,
        scored_more_10_pts,
        lag(scored_more_10_pts, 1) over(partition by player_id order by game_date_est) as lag_scored_more_10_pts,
        case
            when lag(scored_more_10_pts, 1) over(partition by player_id order by game_date_est) <> scored_more_10_pts
            then 1 else 0
        end streak_add
    from scores
),
streak_count as (
    select
        game_id,
        player_id,
        player_name,
        pts,
        game_date_est,
        scored_more_10_pts,
        lag_scored_more_10_pts,
        sum(case when scored_more_10_pts = 1 then streak_add else 0 end) over(partition by player_id order by game_date_est) as streak_10_pts
    from streaks
),
max_streak as (
    select
        player_id,
        player_name,
        streak_10_pts as streak_10_pts_id,
        sum(scored_more_10_pts) as games_streak_more_10_pts
    from streak_count
    group by player_id, player_name, streak_10_pts
)

select
    player_id,
    player_name,
    max(games_streak_more_10_pts) as games_streak_more_10_pts
from max_streak
group by player_id, player_name

