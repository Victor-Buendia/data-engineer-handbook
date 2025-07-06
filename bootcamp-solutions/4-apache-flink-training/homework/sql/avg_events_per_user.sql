SELECT
    ip,
    AVG(event_count) AS avg_events
FROM print_sink
GROUP BY ip