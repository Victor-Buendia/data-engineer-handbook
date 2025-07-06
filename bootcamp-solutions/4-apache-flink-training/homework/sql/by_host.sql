SELECT
    host,
    AVG(event_count) AS avg_events
FROM print_sink
GROUP BY host