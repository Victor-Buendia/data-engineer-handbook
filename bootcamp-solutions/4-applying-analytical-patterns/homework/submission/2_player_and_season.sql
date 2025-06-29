with game_dets as (
    select
        coalesce(cast(gd.player_id as text), 'null') as player_id,
        coalesce(cast(gd.player_name as text), 'null') as player_name,
        coalesce(cast(gd.team_id as text), 'null') as team_id,
        coalesce(cast(gd.team_abbreviation as text), 'null') as team_abbreviation,
        coalesce(cast(gd.game_id as text), 'null') as game_id,
        coalesce(cast(g.season as text), 'null') as season,
        (
            (g.home_team_id = gd.team_id and g.home_team_wins = 1)
            or (g.home_team_id <> gd.team_id and g.home_team_wins = 0)
        ) as team_id_won_game,
        coalesce(gd.pts, 0) as pts
    from game_details gd
    join games g
        on gd.game_id = g.game_id
),
sets as (
    select
        coalesce(player_id, '(OVERALL)') as player_id,
        coalesce(player_name, '(OVERALL)') as player_name,
        coalesce(team_id, '(OVERALL)') as team_id,
        coalesce(team_abbreviation, '(OVERALL)') as team_abbreviation,
        coalesce(season, '(OVERALL)') as season,
        sum(pts) as scored_pts,
        sum(case when team_id_won_game then 1 else 0 end) as games_won
    from game_dets
    group by grouping sets (
        (player_id, player_name, team_id, team_abbreviation),
        (player_id, player_name, season),
        (team_id, team_abbreviation)
    )
)

select * from sets
where team_id = '(OVERALL)'
order by scored_pts desc
limit 1