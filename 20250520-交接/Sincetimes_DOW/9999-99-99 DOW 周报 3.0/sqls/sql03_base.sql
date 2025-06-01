/*
* @Author: dingyelen
* @Date:   2024-12-13 18:18:26
* @Last Modified by:   dingyelen
* @Last Modified time: 2024-12-17 13:33:01
*/

###
with month_tag as(
select role_id, month, is_test, pay_tag
from hive.dow_jpnew_w.dws_user_info_mi
where month = date $pay_month
), 

user_daily as(
select a.date, a.role_id, 
coalesce(c.pay_tag, '未活跃') as pay_tag, 
-- coalesce(d.pay_tag, '未活跃') as last1_tag, 
b.is_pay, a.money, a.money * 0.052102 as money_rmb, a.pay_count
from hive.dow_jpnew_w.dws_user_daily_di a
left join hive.dow_jpnew_w.dws_user_daily_derive_di b
on a.role_id = b.role_id and a.date = b.date
left join month_tag c
on a.role_id = c.role_id
-- left join month_tag d
-- on a.role_id = d.role_id and date_trunc('month', a.date) = date_add('month', 1, d.month)
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
role_id, pay_tag, 
-- last1_tag, 
is_pay, money_rmb, pay_count
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
role_id, pay_tag, 
-- last1_tag, 
is_pay, money_rmb, pay_count
from date_process_cal01
), 

date_process_info as(
select date, month, 
natualweek_turn_month, start_natualweek, end_natualweek, 
start_week, end_week, 
concat(date_format(start_natualweek, '%m%d'), '-', date_format(end_natualweek, '%m%d')) as natual_week, 
concat(date_format(start_week, '%m%d'), '-', date_format(end_week, '%m%d')) as week, 
role_id, pay_tag, 
-- last1_tag, 
is_pay, money_rmb, pay_count
from date_process_cal02
), 

monthly_agg as(
select month, pay_tag, count(distinct role_id) as last_mau
from date_process_info
where month = date $pay_month
group by 1, 2
), 

weekly_info as(
select month, start_week, end_week, week, 
pay_tag, role_id, 
sum(pay_count) as weekly_paycount
from date_process_info
group by 1, 2, 3, 4, 5, 6
), 

weekly_agg as(
select month, start_week, end_week, week, pay_tag, 
count(distinct role_id) as wau, 
count(distinct case when weekly_paycount>0 then role_id else null end) is_weeklypay
from weekly_info
group by 1, 2, 3, 4, 5
)

select a.start_week, a.end_week, a.week, a.pay_tag, 
b.last_mau, a.wau, a.is_weeklypay
from weekly_agg a
left join monthly_agg b
on a.pay_tag = b.pay_tag;
###