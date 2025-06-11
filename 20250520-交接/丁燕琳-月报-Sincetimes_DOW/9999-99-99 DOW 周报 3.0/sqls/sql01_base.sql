/*
* @Author: dingyelen
* @Date:   2024-12-13 16:43:24
* @Last Modified by:   dingyelen
* @Last Modified time: 2024-12-17 14:56:55
*/

###
with month_tag as(
select role_id, month, is_test, pay_tag
from hive.dow_jpnew_w.dws_user_info_mi
), 

user_daily as(
select a.date, a.role_id, 
b.is_pay, a.money, a.web_money, 
a.money * 0.052102 as money_rmb, a.web_money * 0.052102 as webmoney_rmb, 
a.core_add, a.core_cost, b.core_end, 
a.sincetimes_add, a.sincetimes_cost, b.sincetimes_end
from hive.dow_jpnew_w.dws_user_daily_di a
left join hive.dow_jpnew_w.dws_user_daily_derive_di b
on a.role_id = b.role_id and a.date = b.date
left join hive.dow_jpnew_w.dws_user_info_di z
on a.role_id = z.role_id 
where a.part_date >= $start_date
and a.part_date <= $end_date
and a.is_test is null
), 

date_process_cal01 as(
select date, date_trunc('month', date) as month, 
last_day_of_month(date) as last_of_month, 
date_trunc('month', date_add('day', 6, date_trunc('week', date))) as natualweek_turn_month, 
date_trunc('week', date) as start_natualweek, 
date_add('day', 6, date_trunc('week', date)) as end_natualweek, 
7-day_of_week(date) as sunday_diff, 
day_of_week(date)-1 as monday_diff, 
role_id, is_pay, money_rmb, webmoney_rmb, 
core_add, core_cost, core_end, 
sincetimes_add, sincetimes_cost, sincetimes_end
from user_daily
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
role_id, is_pay, money_rmb, webmoney_rmb, 
core_add, core_cost, core_end, 
sincetimes_add, sincetimes_cost, sincetimes_end
from date_process_cal01
), 

date_process_info as(
select date, a.month, 
natualweek_turn_month, start_natualweek, end_natualweek, 
start_week, end_week, 
concat(date_format(start_natualweek, '%m%d'), '-', date_format(end_natualweek, '%m%d')) as natual_week, 
concat(date_format(start_week, '%m%d'), '-', date_format(end_week, '%m%d')) as week, 
a.role_id, coalesce(c.pay_tag, '未活跃') as last1_paytag, is_pay, money_rmb, webmoney_rmb, 
core_add, core_cost, core_end, 
sincetimes_add, sincetimes_cost, sincetimes_end
from date_process_cal02 a
left join month_tag c
on a.role_id = c.role_id and a.natualweek_turn_month = date_add('month', 1, c.month)
), 

core_log_base as(
select part_date, date(part_date) as date, event_time, 
role_id, open_id, adid, 
zone_id, vip_level, level, rank_level, 
reason, event_type, 
coalesce(free_num, 0) as free_num, coalesce(paid_num, 0) as paid_num, 
coalesce(free_end, 0) as free_end, coalesce(paid_end, 0) as paid_end
from hive.dow_jpnew_r.dwd_gserver_corechange_live
where part_date >= $start_date
and part_date <= $end_date
and reason != '638'
), 

core_log as(
select part_date, date, event_time, 
role_id, open_id, adid, 
zone_id, vip_level, level, rank_level, reason, 
(case when event_type = 'gain' then free_num + paid_num else null end) as core_add, 
(case when event_type = 'cost' then free_num + paid_num else null end) as core_cost
from core_log_base
), 

core_info as(
select part_date, date, role_id, 
sum(case when reason in('4', '933', '192', '399', '362', '307', '932', '20300', '10052', '10079', '194', '245', '937', '19601', '19602') then core_add else null end) as paid_add, 
sum(case when reason in('387', '601', '602', '196', '310', '372', '239', '13001', '207', '240', '191', '193', '305', '101', '608', '678', '306', '2', '311', '680', '171', '17001', '10071', '354', '103') then core_add else null end) as free_add, 
sum(case when reason in('15', '316') then core_add else null end) as special_add, 
sum(case when reason not in ('4', '933', '192', '399', '362', '307', '932', '20300', '10052', '10079', '194', '245', '937', '19601', '19602', '15', '316', '387', '601', '602', '196', '310', '372', '239', '13001', '207', '240', '191', '193', '305', '101', '608', '678', '306', '2', '311', '680', '171', '17001', '10071', '354', '103') then core_add else null end) as other_add, 
sum(case when reason not in('388', '389') then core_cost else null end) as special_cost, 
sum(case when reason in('388', '389') then core_cost else 0 end) as other_cost
from core_log
group by 1, 2, 3
), 

daily_info as(
select a.date, a.month, 
a.natualweek_turn_month, a.start_natualweek, a.end_natualweek, 
a.start_week, a.end_week, a.natual_week, a.week, 
a.role_id, a.last1_paytag, a.is_pay, a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
b.paid_add, b.free_add, b.special_add, 
b.other_add, b.special_cost, b.other_cost,  
row_number() over(partition by a.role_id, a.natual_week order by a.date desc) as rn
from date_process_info a
left join core_info b
on a.role_id = b.role_id and a.date = b.date
), 

natualweekly_info as(
select natual_week, role_id, 
sum(is_pay) as natural_paycount
from daily_info
group by 1, 2
)

select a.date, a.month, 
a.natualweek_turn_month, a.start_natualweek, a.end_natualweek, 
a.start_week, a.end_week, a.natual_week, a.week, 
a.role_id, a.last1_paytag, 
a.is_pay, (case when rn = 1 and b.natural_paycount > 0 then 1 else null end) as is_weeklypay, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
a.paid_add, a.free_add, a.special_add, 
a.other_add, a.special_cost, a.other_cost, 
1 as dau, (case when rn = 1 then 1 else null end) as wau, rn
from daily_info a
left join natualweekly_info b
on a.role_id = b.role_id and a.natual_week = b.natual_week;
###