insert into host_activity_reduced
with today_date as (
    select date('2023-01-01') as today
),
today as (
    select
        host,
        count(distinct user_id) as distinct_users,
        count(1) as hits,
        date(event_time) as date
    from events
    where date(event_time) = (select today from today_date)
    group by host, date(event_time)
),
yesterday as (
    select *
    from host_activity_reduced
    where month = (select date(date_trunc('month',today)) from today_date)
),
reduced as (
    select
        coalesce(y.month, date(date_trunc('month', t.date))) as month,
        coalesce(t.host, y.host) as host,
        case
            -- hosts only in today
            when y.host is null then array_fill(0, array[t.date - date(date_trunc('month', t.date))]) || array[t.hits]
            -- hosts only in yesterday
            when t.host is null then y.hit_array || array[0]
            -- hosts in yesterday and today
            else y.hit_array || array[hits]
        end as hit_array,
        case
            -- hosts only in today
            when y.host is null then array_fill(0, array[t.date - date(date_trunc('month', t.date))]) || array[t.distinct_users]
            -- hosts only in yesterday
            when t.host is null then y.unique_visitors || array[0]
            -- hosts in yesterday and today
            else y.unique_visitors || array[distinct_users]
        end as unique_visitors
    from today t
    full outer join yesterday y
        on t.host = y.host
)

select * from reduced
on conflict (host, month) do update
set hit_array = excluded.hit_array, unique_visitors = excluded.unique_visitors;