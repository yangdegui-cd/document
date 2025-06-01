/*
* @Author: dingyelen
* @Date:   2024-12-16 14:23:33
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-03-17 11:42:15
*/

###
with month_tag as(
select role_id, month, is_test, pay_tag
from hive.dow_jpnew_w.dws_user_info_mi
), 

core_log_base as(
select part_date, date(part_date) as date, event_time, 
a.role_id, open_id, adid, 
zone_id, vip_level, level, rank_level, 
reason, reason_cn, event_type, 
coalesce(free_num, 0) as free_num, coalesce(paid_num, 0) as paid_num, 
coalesce(free_end, 0) as free_end, coalesce(paid_end, 0) as paid_end
from hive.dow_jpnew_r.dwd_gserver_corechange_live a
left join hive.dow_jpnew_w.dim_gserver_addgold_reason b
on a.reason = b.id
where part_date >= $start_date
and part_date <= $end_date
and reason != '638'
), 

core_log as(
select part_date, date, event_time, 
role_id, open_id, adid, 
zone_id, vip_level, level, rank_level, 
(case when reason in('4', '933', '192', '399', '362', '307', '932', '20300', '10052', '10079', '194', '245', '937', '19601', '19602') then 'gold_paid' 
when reason in('387', '601', '602', '196', '310', '372', '239', '13001', '207', '240', '191', '193', '305', '101', '608', '678', '306', '2', '311', '680', '171', '17001', '10071', '354', '103') then 'gold_free'
when reason in('15', '316') then 'special'
else 'other' end) as reason_type, reason, reason_cn, 
(case when event_type = 'gain' then free_num + paid_num else null end) as core_add, 
(case when event_type = 'cost' then free_num + paid_num else null end) as core_cost
from core_log_base
), 

date_process_cal01 as(
select date, date_trunc('month', date) as month, 
last_day_of_month(date) as last_of_month, 
date_trunc('month', date_add('day', 6, date_trunc('week', date))) as natualweek_turn_month, 
date_trunc('week', date) as start_natualweek, 
date_add('day', 6, date_trunc('week', date)) as end_natualweek, 
7-day_of_week(date) as sunday_diff, 
day_of_week(date)-1 as monday_diff, 
role_id, reason_type, reason, reason_cn, 
core_add, core_cost
from core_log
), 

date_process_cal02 as(
select date, month, 
last_of_month, natualweek_turn_month, 
start_natualweek, end_natualweek, 
sunday_diff, monday_diff, 
(case when date = month then date 
when date_add('day', -monday_diff, date) < month then month
else start_natualweek end) as start_week, 
(case when date = last_of_month then date 
when date_add('day', sunday_diff, date) > last_of_month then last_of_month
else date_add('day', sunday_diff, date) end) as end_week, 
role_id, reason_type, reason, reason_cn, 
core_add, core_cost
from date_process_cal01
), 

date_process_info as(
select date, a.month, 
natualweek_turn_month, start_natualweek, end_natualweek, 
start_week, end_week, 
concat(date_format(start_natualweek, '%m%d'), '-', date_format(end_natualweek, '%m%d')) as natual_week, 
concat(date_format(start_week, '%m%d'), '-', date_format(end_week, '%m%d')) as week, 
a.role_id, coalesce(c.pay_tag, '未活跃') as last1_paytag, reason_type, reason, reason_cn, 
core_add, core_cost
from date_process_cal02 a
left join month_tag c
on a.role_id = c.role_id and a.natualweek_turn_month = date_add('month', 1, c.month)
)

select start_natualweek, end_natualweek, natual_week, 
reason_type, reason, reason_cn, last1_paytag, 
count(distinct role_id) as users, 
sum(core_add) as core_add, 
sum(core_cost) as core_cost
from date_process_info
group by 1, 2, 3, 4, 5, 6, 7
###