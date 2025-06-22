insert into hosts_cumulated
with today_date as (
    select date('2023-01-01') as today
),
today as (
    select
        host,
        date(event_time) as event_date
    from events
    where date(event_time) = (select today from today_date)
    group by host, date(event_time)
),
yesterday as (
    select *
    from hosts_cumulated
    where event_date = (select (today - interval '1 day') from today_date)
),
cumulated as (
    select
        coalesce(t.host, y.host) as host,
        case
            -- hosts only in today
            when y.host is null then array[t.event_date]
            -- hosts only in yesterday
            when t.host is null then y.host_activity_datelist
            -- hosts that are in yesterday and today
            else array[t.event_date] || y.host_activity_datelist
        end as host_activity_datelist,
        coalesce(t.event_date, y.event_date + 1) as event_date
    from yesterday y
    full outer join today t
        on y.host = t.host
)

select * from cumulated;