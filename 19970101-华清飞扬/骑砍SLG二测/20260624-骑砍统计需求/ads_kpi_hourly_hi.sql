create table if not exists hive.qkslg2t_om_w.ads_kpi_hourly_hi
(date date,
hour timestamp,
zone_id varchar,
channel varchar,
is_old_user int,
os varchar, 
new_users bigint,
active_users bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

create table if not exists hive.qkslg2t_om_w.ads_kpi_hourly_total_hi
(date date,
zone_id varchar,
channel varchar,
is_old_user int,
os varchar, 
total_new_users bigint,
total_active_users bigint,
prev_new_users BIGINT,
prev_active_users BIGINT,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.qkslg2t_om_w.ads_kpi_hourly_hi 
where part_date >= cast(date_add('day', -1, date('{yesterday}')) as varchar )
and part_date <= '{today}';

delete from hive.qkslg2t_om_w.ads_kpi_hourly_total_hi 
where part_date >= cast(date_add('day', -1, date('{yesterday}')) as varchar )
and part_date <= '{today}';

insert into hive.qkslg2t_om_w.ads_kpi_hourly_hi
(date, hour, zone_id, os, channel,is_old_user, 
new_users, 
active_users, 
part_date
)

with base_log as
(select
    part_date,
    event_time,
    event_name,
    first_value(zone_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as zone_id,
    first_value(os_name) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as os,
    first_value(channel) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as channel,
    first_value(adid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as adid,
    role_id,
    case when event_name = 'register' then role_id else null end as r_role_id
from hive.qkslg2t_om_r.dwd_merge_base_live
where part_date >= cast(date_add('day', -1, date('{yesterday}')) as varchar )
and part_date <= '{today}'
),

base_log_join as
(select
    a.*,
    case when b.adid is null then 0 else 1 end as is_old_user
from base_log a
left join (SELECT DISTINCT adid FROM hive.qkslg_om_w.dws_user_info_di) b
on a.adid = b.adid
)

select
    date(part_date) as date,
    date_trunc('hour', event_time) as hour,
    zone_id,
    os,
    channel,
    is_old_user,
    count(distinct r_role_id) as new_users,
    count(distinct role_id) as active_users,
    part_date
from  base_log_join
group by 1,2,3,4,5,6,part_date;



-- Insert into total hourly table

insert into hive.qkslg2t_om_w.ads_kpi_hourly_total_hi
(date, zone_id, os, channel,is_old_user, 
total_new_users,
total_active_users,
prev_new_users,
prev_active_users,
part_date
)

with base_log as
(select
    part_date,
    event_time,
    event_name,
    first_value(zone_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as zone_id,
    first_value(os_name) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as os,
    first_value(channel) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as channel,
    first_value(adid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as adid,
    role_id,
    case when event_name = 'register' then role_id else null end as r_role_id
from hive.qkslg2t_om_r.dwd_merge_base_live
where part_date >= cast(date_add('day', -1, date('{yesterday}')) as varchar )
and part_date <= '{today}'
),

base_log_join as
(select
    a.*,
    case when b.adid is null then 0 else 1 end as is_old_user
from base_log a
left join (SELECT DISTINCT adid FROM hive.qkslg_om_w.dws_user_info_di) b
on a.adid = b.adid
)

select
    date(part_date) as date,
    zone_id,
    os,
    channel,
    is_old_user,
    count(distinct r_role_id) as total_new_users,
    count(distinct role_id) as total_active_users,
    count(distinct case when event_time <= date_add('hour', -1, at_timezone(current_timestamp, 'UTC')) then r_role_id else null end ) as prev_new_users,
    count(distinct case when event_time <= date_add('hour', -1, at_timezone(current_timestamp, 'UTC')) then role_id else null end ) as prev_active_users,
    part_date
from  base_log_join
group by 1,2,3,4,5,part_date