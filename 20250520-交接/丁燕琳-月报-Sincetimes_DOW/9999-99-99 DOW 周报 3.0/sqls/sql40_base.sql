/*
* @Author: dingyelen
* @Date:   2024-12-13 18:18:26
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-05-12 10:42:31
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

battlepass_conf as(
select *
from (values
('gold_9', 30), 
('gold_10', 30), 
('gold_80002', 30)
-- ('gold_80003', 30), 
-- ('gold_80004', 30), 
-- ('gold_80005', 30), 
-- ('gold_80006', 30), 
-- ('gold_80007', 30), 
-- ('gold_80008', 30), 
-- ('gold_80009', 30), 
-- ('gold_301', 30), 
-- ('gold_302', 30), 
-- ('gold_316', 30), 
-- ('gold_317', 30)
) as config(payment_itemid, days)
), 

paymentdetail_log as(
select part_date, date(part_date) as date, event_time, 
role_id, open_id, 
(case when a.payment_item_id in ('gold_80007', 'gold_80004') then 'gold_10'
when a.payment_item_id in ('gold_80008', 'gold_80005') then 'gold_9'
when a.payment_item_id in ('gold_80009', 'gold_80006') then 'gold_80002' else payment_item_id end)as payment_itemid, 
b.payment_act, b.payment_name, 
items_detail, rawmoney * 0.052102 as money_rmb
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live a
left join hive.dow_jpnew_w.dim_gserver_payment_paymentitemid b
on a.payment_item_id = b.payment_itemid
where 
-- a.part_date >= date_format(date_add('day', -60, date $start_date), '%Y-%m-%d')
-- and a.part_date <= $end_date
payment_item_id in ('gold_9', 'gold_10', 'gold_80002', 'gold_80003', 'gold_80004', 'gold_80005', 'gold_80006', 'gold_80007', 'gold_80008', 'gold_80009')
-- payment_item_id in ('gold_80004', 'gold_80005', 'gold_80006', 'gold_80007', 'gold_80008', 'gold_80009')
), 

paymentdetail_join as(
select a.date as pay_date, 
a.role_id, a.payment_itemid, b.days
from paymentdetail_log a
left join battlepass_conf b
on a.payment_itemid = b.payment_itemid
), 

paymentdetail_unnest as(
select date, days, 
role_id, payment_itemid
from paymentdetail_join 
cross join unnest(sequence(pay_date, date_add('day', days-1, pay_date), interval '1' day)) as t(date)
), 

paymentdetail_unnest_agg1 as(
select date, 
role_id, payment_itemid, days, 
count(*) as date_count
from paymentdetail_unnest
group by 1, 2, 3, 4
), 

paymentdetail_unnest_agg_rn as(
select *, date_add('day', -rn, date) as paydate_group
from(
select date, days, 
role_id, payment_itemid, 
date_count, date_count - 1 as day_remain, 
row_number() over(partition by role_id, payment_itemid order by date) as rn
from paymentdetail_unnest_agg1)
), 

paymentdetail_unnest_agg2 as(
select role_id, payment_itemid, days, paydate_group, 
-- min(date) as pay_date, 
max(date) as max_date, 
sum(day_remain) as day_remain
from paymentdetail_unnest_agg_rn
group by 1, 2, 3, 4
), 

paymentdetail_unnest_fill as(
select date, days, 
role_id, payment_itemid
from paymentdetail_unnest_agg2
cross join unnest(sequence(date_add('day', 1, max_date), date_add('day', day_remain, max_date), interval '1' day)) as t(date)
where day_remain > 0
), 

union_unnest as(
select distinct date, days, 
role_id, payment_itemid
from(
select * from paymentdetail_unnest
union all
select * from paymentdetail_unnest_fill)
), 

union_select as(
select date, days, 
role_id, payment_itemid
from union_unnest
where date >= date_add('day', -60, date $start_date)
and date <= date $end_date
), 

battlepass_valid as(
select date, role_id, 
sum(case when payment_itemid = 'gold_9' then 1 else null end) as is_9, 
sum(case when payment_itemid = 'gold_10' then 1 else null end) as is_10, 
sum(case when payment_itemid = 'gold_80002' then 1 else null end) as is_80002
-- sum(case when payment_itemid = 'gold_80003' then 1 else null end) as is_80003, 
-- sum(case when payment_itemid = 'gold_80004' then 1 else null end) as is_80004, 
-- sum(case when payment_itemid = 'gold_80005' then 1 else null end) as is_80005, 
-- sum(case when payment_itemid = 'gold_80006' then 1 else null end) as is_80006
-- sum(case when payment_itemid = 'gold_301' then 1 else null end) as is_301, 
-- sum(case when payment_itemid = 'gold_302' then 1 else null end) as is_302, 
-- sum(case when payment_itemid = 'gold_316' then 1 else null end) as is_316, 
-- sum(case when payment_itemid = 'gold_317' then 1 else null end) as is_317
from union_select 
group by 1, 2
), 

res as(
select a.date, a.role_id, a.pay_tag, 
a.is_pay, b.is_9, b.is_10, b.is_80002, 
-- b.is_80003, 
-- b.is_80004, b.is_80005, b.is_80006, 
-- b.is_301, b.is_302, b.is_316, b.is_317, 
1 as dau
from user_daily a
left join battlepass_valid b
on a.date = b.date and a.role_id = b.role_id
)

select date, pay_tag, 
sum(is_pay) as is_pay, 
sum(is_9) as is_9, 
sum(is_10) as is_10, 
sum(is_80002) as is_80002, 
-- sum(is_80003) as is_80003, 
-- sum(is_80004) as is_80004, 
-- sum(is_80005) as is_80005, 
-- sum(is_80006) as is_80006, 
-- sum(is_301) as is_301, 
-- sum(is_302) as is_302, 
-- sum(is_316) as is_316, 
-- sum(is_317) as is_317, 
sum(dau) as dau
from res
group by 1, 2;
###
-- check1 as(
-- select role_id, payment_itemid, 
-- count(*) as days
-- from union_unnest
-- group by 1, 2
-- ), 

-- check2 as(
-- select role_id, payment_itemid, 
-- sum(days) as days
-- from paymentdetail_join
-- group by 1, 2
-- )

-- select a.*, b.days
-- from check1 a
-- left join check2 b
-- on a.role_id = b.role_id and a.payment_itemid = b.payment_itemid
-- where a.days != b.days

-- select
-- from paymentdetail_unnest_fill
-- where role_id = '15680925770448'

