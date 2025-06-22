create type datelist as
(
    browser_type TEXT,
    datelist_int BIT(31)
);

with date_series as (
    select
        date(
                generate_series(
                        (select min(event_date) - interval '30 day' from user_devices_cumulated),
                        (select max(event_date) from user_devices_cumulated),
                        interval '1 day'
                )
        ) as date_serie
),
active_days as (
    select
        udc.user_id,
        (unnest(device_activity_datelist)::device_activity_datelist).browser_type,
        (unnest(device_activity_datelist)::device_activity_datelist).active_dates,
        ds.date_serie,
        array[date_serie] <@ (unnest(device_activity_datelist)::device_activity_datelist).active_dates as dim_is_active_today,
        event_date - date_serie as days_since_date_serie,
        udc.event_date
    from user_devices_cumulated udc
            join date_series ds
                on ds.date_serie >= (udc.event_date - interval '30 day')
                    and ds.date_serie <= udc.event_date
    --         where udc.user_id = '10060569187331700000' --and udc.event_date = '2023-01-31'
    where udc.event_date = '2023-01-31'
),
power_2_history as (
    select
        user_id,
        date_serie,
        browser_type,
        active_dates,
        dim_is_active_today,
        days_since_date_serie,
        case
            when dim_is_active_today
                then pow(2, 30 - days_since_date_serie)
            else 0
            end as power_2_history,
        case
            when dim_is_active_today
                then pow(2, 30 - days_since_date_serie)::bigint::bit(31)
            else 0::int::bit(31)
            end as power_2_history_bit,
        event_date
    from active_days
),
int_list as (
    select
        user_id,
        browser_type,
        active_dates,
        sum(power_2_history) as power_2_sum_history,
        sum(power_2_history)::bigint::bit(31) as datelist_int,
    --         bit_count(sum(power_2_history)::bigint::bit(31)) as datelist_int_count,
        event_date
    from power_2_history
    group by user_id, browser_type, active_dates, event_date
    ),
    int_list_by_browser as (
    select
        user_id,
        array_agg(row(browser_type, datelist_int)::datelist) as datelist_int,
        event_date
    from int_list
    group by user_id, event_date
    ),
    unnested_int_list_by_browser as (
    select
        user_id,
        datelist_int,
        (unnest(datelist_int)::datelist).datelist_int as int_active_days,
        (unnest(datelist_int)::datelist).browser_type,
        event_date
    from int_list_by_browser
)

select
    user_id,
    datelist_int,
    bit_or(int_active_days) as dim_int_activity_last_30_days,
    bit_count(bit_or(int_active_days)) as dim_active_days_count_last_30_days,
    bit_count(bit_or(int_active_days)) > 1 as dim_is_active_this_month,
    bit_count(
        bit_or(int_active_days)
        & '1111111000000000000000000000000'::bit(31)
    ) > 1 as dim_is_active_this_week,
    bit_count(
        bit_or(int_active_days)
        & '1000000000000000000000000000000'::bit(31)
    ) > 1 as dim_is_active_today,
    event_date
from unnested_int_list_by_browser
-- where user_id = '10060569187331700000'
-- and event_date = '2023-01-31'
-- and browser_type = 'Crawlson'
group by user_id, datelist_int, event_date
;
