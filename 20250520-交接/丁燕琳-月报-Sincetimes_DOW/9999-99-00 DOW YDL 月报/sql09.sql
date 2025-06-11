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

payment_log as(
select part_date, date(part_date) as date, role_id, 
a.payment_itemid, b.payment_cate, money
from hive.dow_jpnew_r.dwd_gserver_payment_live a
left join hive.dow_jpnew_w.dim_gserver_payment_paymentitemid b
on a.payment_itemid = b.payment_itemid
where a.part_date >= '2024-06-01'
and a.part_date <= '2025-04-30'
), 

payment_agg as(
select part_date, date, role_id
from payment_log
where payment_cate not in ('珍珠', '网页支付')
group by 1, 2, 3
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
(case when c.role_id is not null then 1 else null end) as is_totalpay, 
1 as dau
from hive.dow_jpnew_w.dws_user_daily_di a
-- left join hive.dow_jpnew_w.dws_user_daily2_di b
-- on a.role_id = b.role_id 
-- and a.part_date = b.part_date
left join payment_agg c
on a.role_id = c.role_id
and a.part_date = c.part_date
left join month_tag z
on a.role_id = z.role_id 
and date_trunc('month', date(a.part_date)) = z.month
where a.part_date >= '2024-06-01'
and a.part_date <= '2025-04-30'
and z.is_test is null
)

select part_date, date, month,  
count(distinct role_id) as dau, 
sum(is_pay) as pay_dau, 
sum(is_totalpay) as paytotal_dau
from user_daily 
group by 1, 2, 3
