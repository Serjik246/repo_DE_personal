CREATE DATABASE events;


CREATE TABLE events.user_events (
        user_id UInt32,
        event_type String,
        points_spent UInt32,
        event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;


CREATE TABLE events.user_events_agg (
        event_date Date,
        event_type String,
        uniq_users_state AggregateFunction(uniq, UInt32),
        sum_points_spent_state AggregateFunction(sum, UInt32),
        count_state AggregateFunction(count, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;


CREATE MATERIALIZED VIEW events.mv_user_daily_agg
TO events.user_events_agg
as
select
    toDate(event_time) as event_date,
    event_type as event_type,
    uniqState(user_id) as uniq_users_state,
    sumState(points_spent) as sum_points_spent_state,
    countState() as count_state
from events.user_events
group by event_date, event_type;

--Запрос с группировками по быстрой аналитике по дням
select
    event_date,
    event_type,
    uniqMerge(uniq_users_state) as uniq_users,
    sumMerge(sum_points_spent_state) as total_spent,
    countMerge(count_state) as total_actions
from events.user_events_agg
group by event_date, event_type;

--Запрос показывающий Retention
with first_user_event as (
    select
        user_id,
        min(event_time) as first_date
    from events.user_events
    group by user_id
),
     retention_table as (
         select
             fue.user_id,
             retention(
                dateDiff('day', first_date, event_time) = 0,
                dateDiff('day', first_date, event_time) between 1 and 8
             ) as ret_arr
         from first_user_event fue
         inner join events.user_events ue on ue.user_id = fue.user_id
         group by fue.user_id
     )
select
    sum(ret_arr[1]) as total_users_day_0,
    sum(ret_arr[2]) as returned_in_7_days,
    round(returned_in_7_days / total_users_day_0 * 100) as retention_7d_percent
from retention_table;