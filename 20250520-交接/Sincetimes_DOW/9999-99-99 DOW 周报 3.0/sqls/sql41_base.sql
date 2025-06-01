/*
* @Author: dingyelen
* @Date:   2024-12-16 10:42:14
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-04-29 14:35:58
*/

###
with month_tag as(
select role_id, month, is_test, pay_tag
from hive.dow_jpnew_w.dws_user_info_mi
),  

paymentdetail_log as(
select part_date, date(part_date) as date, event_time, 
role_id, open_id, 
a.payment_item_id as payment_itemid, b.payment_act, b.payment_name, 
items_detail, rawmoney * 0.052102 as money_rmb, sincetime_money
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live a
left join hive.dow_jpnew_w.dim_gserver_payment_paymentitemid b
on a.payment_item_id = b.payment_itemid
where part_date >= $start_date
and part_date <= $end_date
), 

payment_daily_info as(
select part_date, date, role_id, 
payment_itemid, payment_act, payment_name, 
sum(money_rmb) as money_rmb, 
sum(sincetime_money) as sincetime_money
from paymentdetail_log
group by 1, 2, 3, 4, 5, 6
), 

date_process_cal01 as(
select date, date_trunc('month', date) as month, 
last_day_of_month(date) as last_of_month, 
date_trunc('month', date_add('day', 6, date_trunc('week', date))) as natualweek_turn_month, 
date_trunc('week', date) as start_natualweek, 
date_add('day', 6, date_trunc('week', date)) as end_natualweek, 
7-day_of_week(date) as sunday_diff, 
day_of_week(date)-1 as monday_diff, 
role_id, payment_itemid, payment_act, payment_name, 
money_rmb, sincetime_money
from payment_daily_info
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
role_id, payment_itemid, payment_act, payment_name, 
money_rmb, sincetime_money
from date_process_cal01
), 

date_process_info as(
select date, a.month, 
natualweek_turn_month, start_natualweek, end_natualweek, 
start_week, end_week, 
concat(date_format(start_natualweek, '%m%d'), '-', date_format(end_natualweek, '%m%d')) as natual_week, 
concat(date_format(start_week, '%m%d'), '-', date_format(end_week, '%m%d')) as week, 
a.role_id, coalesce(c.pay_tag, '未活跃') as last1_paytag, 
payment_itemid, payment_act, payment_name, 
money_rmb, sincetime_money
from date_process_cal02 a
left join month_tag c
on a.role_id = c.role_id and a.natualweek_turn_month = date_add('month', 1, c.month)
), 

group_cal01 as(
select 'paymentname_date_group' as type, 
cast(date as varchar) as period, last1_paytag, 
payment_act as dtype1, payment_name as dtype2, 
count(distinct role_id) as users, 
sum(money_rmb) as money_rmb, 
sum(sincetime_money) as sincetime_money
from date_process_info
group by 1, 2, 3, 4, 5
), 

group_cal02 as(
select 'paymentname_week_group' as type, 
natual_week as period, last1_paytag, 
payment_act as dtype1, payment_name as dtype2, 
count(distinct role_id) as users, 
sum(money_rmb) as money_rmb, 
sum(sincetime_money) as sincetime_money
from date_process_info
group by 1, 2, 3, 4, 5
), 

group_cal03 as(
select 'paymentact_week_group' as type, 
natual_week as period, last1_paytag, 
payment_act as dtype1, null as dtype2, 
count(distinct role_id) as users, 
sum(money_rmb) as money_rmb, 
sum(sincetime_money) as sincetime_money
from date_process_info
group by 1, 2, 3, 4, 5
)

-- select * from group_cal01
-- union all
-- select * from group_cal02
-- union all
select * from group_cal03
###