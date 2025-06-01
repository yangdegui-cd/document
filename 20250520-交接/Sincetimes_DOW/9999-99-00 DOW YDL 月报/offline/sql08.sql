with month_tag as(
select month, role_id, pay_tag, is_test
from hive.dow_jpnew_w.dws_user_info_mi
), 

user_tag as(
select role_id, 
install_date, lastlogin_ts, lastpay_ts, 
is_paid, level, vip_level, is_test
from hive.dow_jpnew_w.dws_user_info_di
), 

user_daily as(
select a.part_date, date(a.part_date) as date, 
date_trunc('month', date(a.part_date)) as month, 
a.role_id, z.pay_tag, level_min, level_max, power_min, power_max, 
online_time, login_times, 
a.money, a.money * 0.052102 as money_rmb, app_money, web_money,
pay_count, app_count, web_count,  
sincetimes_add, sincetimes_cost, sincetimes_end, 
-- pvp_count, pvp_win, pvp_alliance, pve_count, 
-- huodong_count, huodong_win, 
(case when a.money>0 then 1 else null end) as is_pay, 
1 as dau
from hive.dow_jpnew_w.dws_user_daily_di a
-- left join hive.dow_jpnew_w.dws_user_daily2_di b
-- on a.role_id = b.role_id 
-- and a.part_date = b.part_date
left join month_tag z
on a.role_id = z.role_id 
and date_trunc('month', date(a.part_date)) = z.month
where a.part_date >= '2024-06-01'
and a.part_date <= '2025-04-30'
and z.is_test is null
), 

daily_agg as(
select part_date, date, month,  
count(distinct role_id) as dau, 
sum(is_pay) as pay_dau 
from user_daily 
group by 1, 2, 3
), 

payment_log as(
select part_date, date(part_date) as date, date_trunc('month', date(part_date)) as month, 
event_time, a.role_id, z.pay_tag, 
a.payment_item_id as payment_itemid, b.payment_cate, b.payment_name, 
-- a.payment_itemid, b.payment_cate, b.payment_name, 
coalesce(rawmoney*0.052102, 0) + coalesce(sincetime_money, 0) as money_rmb
-- coalesce(money*0.052102, 0) as money_rmb
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live a
left join hive.dow_jpnew_w.dim_gserver_payment_paymentitemid b
on a.payment_item_id = b.payment_itemid
-- on a.payment_itemid = b.payment_itemid
left join month_tag z
on a.role_id = z.role_id 
and date_trunc('month', date(a.part_date)) = z.month
where a.part_date >= '2024-06-01'
and a.part_date <= '2025-04-30' 
and z.is_test is null
and b.payment_cate != '珍珠'
), 

payment_rank as(
select part_date, date, month, event_time, role_id, pay_tag, 
payment_itemid, payment_cate, payment_name, money_rmb, 
row_number() over(partition by role_id, part_date order by event_time) as rn
from payment_log
), 

payment_agg as(
select part_date, date, month, 
payment_itemid, payment_cate, payment_name, 
-- pay_tag, 
count(distinct role_id) as pay_user, 
count(*) as pay_count, 
count(case when rn=1 then role_id else null end) as pay_user_first
from payment_rank
group by 1, 2, 3, 4, 5, 6
), 

payment_agg2 as(
select part_date, date, month, 
payment_cate, 
-- pay_tag, 
count(distinct role_id) as pay_user_cate, 
count(*) as pay_count_cate, 
count(case when rn=1 then role_id else null end) as pay_user_first_cate
from payment_rank
group by 1, 2, 3, 4
), 

demension_payment_name_res as(
select a.part_date, a.date, a.month, 
a.payment_itemid, a.payment_cate, a.payment_name, 
-- a.pay_tag, 
b.dau, b.pay_dau, 
a.pay_user, a.pay_count, a.pay_user_first, 
row_number() over(partition by payment_cate, a.part_date) as rn
from payment_agg a
left join daily_agg b
on a.date = b.date
-- and a.pay_tag = b.pay_tag
)

select a.part_date, a.date, a.month, 
a.payment_itemid, a.payment_cate, a.payment_name, 
-- a.pay_tag, 
a.dau, a.pay_dau, 
a.pay_user, a.pay_count, a.pay_user_first, 
c.dau as dau_cate, c.pay_dau as pay_dau_cate, 
b.pay_user_cate, b.pay_count_cate, b.pay_user_first_cate
from demension_payment_name_res a
left join payment_agg2 b
on a.date = b.date
and a.payment_cate = b.payment_cate
-- and a.pay_tag = b.pay_tag
and a.rn = 1
left join daily_agg c
on a.date = c.date
-- and a.pay_tag = c.pay_tag
and a.rn = 1
