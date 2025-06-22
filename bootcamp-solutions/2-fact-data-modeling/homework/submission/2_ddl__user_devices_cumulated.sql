CREATE TYPE device_activity_datelist AS (
    browser_type TEXT,
    active_dates DATE[]
);

CREATE TABLE IF NOT EXISTS user_devices_cumulated (
    user_id TEXT,
    device_activity_datelist device_activity_datelist[],
    event_date DATE,
    PRIMARY KEY (user_id, event_date)
);