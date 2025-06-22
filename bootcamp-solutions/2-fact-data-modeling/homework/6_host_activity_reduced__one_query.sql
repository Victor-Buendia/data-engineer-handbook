with date_interval as (
    select
        min(date(event_time)) as min_date,
        max(date(event_time)) as max_date
    from events
) ,
date_series as (
    select date(generate_series(
        (select min_date from date_interval),
        (select max_date from date_interval),
        '1 day'
    )) as date_serie
),
host_first_date as (
    select
        host,
        min(date(event_time)) as first_date
    from events
    group by host
),
host_and_dates as (
    select
        h.host,
        d.date_serie
    from date_series d
    left join host_first_date h
        on d.date_serie >= h.first_date
),
hits as (
    select
        h.host,
        count(e.host) as hits,
        count(distinct e.user_id) as distinct_users,
        h.date_serie
    from host_and_dates h
    left join events e
        on h.host = e.host
        and h.date_serie = date(e.event_time)
    group by h.host , h.date_serie
),
reduced_by_day as (
    select
        date(date_trunc('month', date_serie)) as month,
        date_serie,
        host,
        array_agg(hits) over(partition by host, date(date_trunc('month', date_serie)) order by date_serie) as hit_array,
        array_agg(distinct_users) over(partition by host, date(date_trunc('month', date_serie)) order by date_serie) as unique_visitors,
        row_number() over(partition by host, date(date_trunc('month', date_serie)) order by date_serie desc) = 1 as last_record
    from hits
),
reduced as (
    select
        month,
        host,
        hit_array,
        unique_visitors
    from reduced_by_day
    where last_record
)

select * from reduced;