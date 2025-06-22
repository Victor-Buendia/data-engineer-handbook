CREATE MATERIALIZED VIEW IF NOT EXISTS deduped_game_details AS
WITH deduped_game_details AS (
    SELECT
        row_number() over (partition by game_id, team_id, player_id) as row_num,
        *
    FROM game_details
)

SELECT
    game_id,
    team_id,
    team_abbreviation,
    team_city,
    player_id,
    player_name,
    nickname,
    start_position,
    comment,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO" as turnover,
    pf,
    pts,
    plus_minus
FROM deduped_game_details WHERE row_num = 1;