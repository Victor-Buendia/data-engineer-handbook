CREATE MATERIALIZED VIEW IF NOT EXISTS events_with_device AS (
    WITH ev AS (
        SELECT
            TEXT(user_id) AS user_id,
            DATE(event_time) AS event_time,
            device_id
        FROM events
        GROUP BY 1,2,3
    )
    SELECT
        e.user_id,
        e.event_time,
        e.device_id,
        d.browser_type
    FROM ev e
    JOIN devices d
        ON e.device_id = d.device_id
    GROUP BY e.user_id, e.event_time, e.device_id, d.browser_type
);

-- Cumulate users active dates by browser in one query
INSERT INTO user_devices_cumulated
WITH date_series AS (
    SELECT DATE(generate_series('2023-01-01', '2023-01-31', interval '1 day')) AS date_serie
),
events_with_browser_type AS (
    SELECT
        user_id,
        event_time,
        browser_type
    FROM events_with_device
    WHERE user_id IS NOT NULL
    GROUP BY user_id, event_time, browser_type
),
users_first_date AS (
    SELECT
        user_id,
        browser_type,
        MIN(event_time) AS first_active_date
    FROM events_with_browser_type
    GROUP BY user_id, browser_type
),
users_dates AS (
    SELECT
        ufd.user_id,
        ufd.browser_type,
        ds.date_serie
    FROM date_series ds
    JOIN users_first_date ufd
        ON ds.date_serie >= ufd.first_active_date
),
cumulated_users_by_browser AS (
    SELECT
        ud.user_id,
        ud.browser_type,
        array_remove(
            array_agg(e.event_time) over(partition by ud.user_id, ud.browser_type order by ud.date_serie),
            null
        ) as active_dates,
        ud.date_serie AS date
    FROM users_dates ud
    LEFT JOIN events_with_browser_type e
        ON ud.user_id = e.user_id
        AND ud.date_serie = e.event_time
        AND ud.browser_type = e.browser_type
),
cumulated_users AS (
    SELECT
        user_id,
        array_agg(row(browser_type, active_dates)::device_activity_datelist) as device_activity_datelist,
        date AS event_date
    FROM cumulated_users_by_browser
    GROUP BY user_id, date
    ORDER BY date
)
SELECT * FROM cumulated_users;
