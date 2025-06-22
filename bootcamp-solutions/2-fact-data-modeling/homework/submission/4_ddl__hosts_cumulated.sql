CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    event_date DATE,
    PRIMARY KEY (host, event_date)
);