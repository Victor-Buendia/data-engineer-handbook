insert into hosts_cumulated
with hosts as (
    select
        host,
        date(event_time) as event_date
    from events
    group by host, date(event_time)
    order by event_date
),
date_series as (
    select
        date(
            generate_series(
                (select min(event_date) from hosts),
                (select max(event_date) from hosts),
                interval '1 day'
            )
        ) as date_serie
),
first_date_hosts as (
    select
        host,
        min(event_date) as first_host_date
    from hosts
    group by host
),
hosts_dates as (
    select
        h.host,
        ds.date_serie
    from date_series ds
    join first_date_hosts h
        on ds.date_serie >= h.first_host_date
),
cumulated_hosts as (
    select
        hd.host,
        array_remove(
            array_agg(h.event_date) over(partition by hd.host order by hd.date_serie),
            null
        ) as host_activity_datelist,
        hd.date_serie as event_date
    from hosts_dates hd
    left join hosts h
        on hd.host = h.host
        and hd.date_serie = h.event_date
)

select * from cumulated_hosts;