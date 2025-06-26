create table if not exists hive.qkslg2t_om_w.dws_user_hourly_hi
(date date,
hour timestamp,
role_id varchar,
zone_id varchar,
channel varchar,
is_old_user int,
money decimal(36, 2), 
app_money decimal(36, 2), 
web_money decimal(36, 2), 
pay_count bigint,
app_count bigint,
web_count bigint,
events array(varchar),
last_event varchar,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.qkslg2t_om_w.dws_user_hourly_hi 
where part_date >= '{yesterday}'
and part_date <= '{today}';

insert into  hive.qkslg2t_om_w.dws_user_hourly_hi
(date, hour, role_id, zone_id, channel, is_old_user,
money, app_money, web_money, 
pay_count, app_count, web_count, 
events, last_event, part_date)

with base_log_before as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, device_id, 
channel, zone_id, alliance_id,  
'qkslg2t_om' as app_id, 
vip_level, level, rank_level, power, 
pay_source, payment_itemid, currency, 
money, online_time
from hive.qkslg2t_om_r.dwd_merge_base_live
where part_date >= '{yesterday}'
and part_date <= '{today}'
), 

base_log as(
    select a.*, CASE 
        WHEN b.adid is null THEN  0
        ELSE 1 
    END as is_old_user
    from base_log_before a
    left join (select DISTINCT adid from hive.qkslg_om_w.dws_user_info_di) b
    on a.adid = b.adid
),

daily_gserver_info as(
select part_date, 
date(part_date) as date, 
date_trunc('hour', event_time) as hour,
role_id, 
app_id, 
zone_id, 
channel, 
is_old_user,
sum(money) as money, 
sum(case when event_name = 'Payment' and pay_source = 'app' then money else null end) as app_money, 
sum(case when event_name = 'Payment' and pay_source = 'web' then money else null end) as web_money, 
sum(case when event_name = 'Payment' then 1 else null end) as pay_count,
sum(case when event_name = 'Payment' and pay_source = 'app' then 1 else null end) as app_count, 
sum(case when event_name = 'Payment' and pay_source = 'web' then 1 else null end) as web_count, 
array_agg(event_name order by event_time) as events,
element_at(array_agg(event_name order by event_time), -1) as last_event
from base_log
group by 1, 2, 3, 4, 5, 6, 7, 8
)

select date, hour, role_id, zone_id, channel, is_old_user,
money, app_money, web_money, 
pay_count, app_count, web_count, 
events, last_event, part_date
from daily_gserver_info
;