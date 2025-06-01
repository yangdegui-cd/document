/*
* @Author: dingyelen
* @Date:   2024-12-18 14:06:50
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-02-17 11:43:04
*/

###
with act_tag as(
select date, start_date, end_date, 
concat(date_format(start_date, '%Y%m%d'), '-', date_format(end_date, '%Y%m%d'), '-', cast(date_diff('day', start_date, end_date)+1 as varchar)) as act_tag, 
date_diff('day', start_date, end_date) + 1 as act_days, act_name
from hive.dow_jpnew_w.dim_gserver_activity_act
cross join unnest(sequence(start_date, end_date, interval '1' day)) as t(date)
where start_date >= date_add('month', -6, current_date)
and code = 'pearlpaid'
), 

month_tag as(
select role_id, month, is_test, pay_tag
from hive.dow_jpnew_w.dws_user_info_mi
where month = date $pay_month
), 

user_daily as(
select a.date, d.act_tag, 
a.role_id, a.viplevel_max as vip_level, a.level_max as level, 
coalesce(c.pay_tag, '未活跃') as pay_tag, b.is_pay, a.money, a.web_money, 
a.money * 0.052102 as money_rmb, a.web_money * 0.052102 as webmoney_rmb, 
a.core_add, a.core_cost, b.core_end, 
a.sincetimes_add, a.sincetimes_cost, b.sincetimes_end
from hive.dow_jpnew_w.dws_user_daily_di a
left join hive.dow_jpnew_w.dws_user_daily_derive_di b
on a.role_id = b.role_id and a.date = b.date
left join month_tag c
on a.role_id = c.role_id
left join act_tag d
on a.date = d.date
left join hive.dow_jpnew_w.dws_user_info_di z
on a.role_id = z.role_id 
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and a.is_test is null
), 

payment_agg as(
select part_date, date(part_date) as date, role_id, 
sum(coalesce(rawmoney, 0) * 0.052102 + coalesce(sincetime_money, 0)) as act_rmb, 
count(*) as act_paycount
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live a
left join hive.dow_jpnew_w.dim_gserver_payment_paymentitemid c
on a.payment_item_id = c.payment_itemid
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and c.payment_act in ('网页充值', '珍珠')
group by 1, 2, 3
), 

item_agg as(
select part_date, date(part_date) as date, role_id, 
sum(item_num) as pearl_add
from hive.dow_jpnew_r.dwd_gserver_itemchange_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id = '2'
and event_type = 'gain'
group by 1, 2, 3
), 

daily_info as(
select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
coalesce(b.act_rmb, 0) + coalesce(a.webmoney_rmb, 0) as act_rmb, b.act_paycount, c.pearl_add, 
sum(c.pearl_add) over(partition by a.role_id, a.act_tag order by a.date) as pearladd_ac, 
row_number() over(partition by a.role_id, a.act_tag order by a.date) as rn
from user_daily a
left join payment_agg b
on a.role_id = b.role_id and a.date = b.date
left join item_agg c
on a.role_id = c.role_id and a.date = c.date
--left join corechange_agg d
--on a.role_id = d.role_id and a.date = d.date
-- left join summon_agg e
-- on a.role_id = e.role_id and a.date = e.date
where a.act_tag != 'unknown'
), 

act_info as(
select act_tag, role_id, 
sum(act_rmb) as act_moneyrmb, 
sum(money_rmb) as money_rmb, 
sum(pearl_add) as act_pearladd
from daily_info
group by 1, 2
)

select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
a.act_rmb, a.act_paycount, a.pearl_add, a.pearladd_ac, 
(case when b.act_pearladd >= 12000 then '[12000, ∞)'
when b.act_pearladd >= 6000 then '[6000,12000)'
when b.act_pearladd >= 3000 then '[3000,6000)'
when b.act_pearladd >= 2000 then '[2000,3000)'
when b.act_pearladd >= 600 then '[600,2000)'
when b.act_pearladd >= 300 then '[300,600)'
when b.act_pearladd >= 150 then '[150,300)'
when b.act_pearladd >= 70 then '[70,150)'
when b.act_pearladd >= 30 then '[30,70)'
when b.act_pearladd > 0 then '[1,30)'
else '[0]' end) as item_tag, 
(case when a.pearladd_ac >= 12000 then '[12000, ∞)'
when a.pearladd_ac >= 6000 then '[6000,12000)'
when a.pearladd_ac >= 3000 then '[3000,6000)'
when a.pearladd_ac >= 2000 then '[2000,3000)'
when a.pearladd_ac >= 600 then '[600,2000)'
when a.pearladd_ac >= 300 then '[300,600)'
when a.pearladd_ac >= 150 then '[150,300)'
when a.pearladd_ac >= 70 then '[70,150)'
when a.pearladd_ac >= 30 then '[30,70)'
when a.pearladd_ac > 0 then '[1,30)'
else '[0]' end) as pearlpaid_tag, 
(case when rn = 1 and b.act_moneyrmb > 0 then 1 else null end) as actrmb_wau, 
(case when b.act_moneyrmb > 0 then 1 else null end) as actrmb_wau_, 
(case when rn = 1 and b.money_rmb > 0 then 1 else null end) as pay_wau, 
(case when a.act_rmb > 0 then 1 else null end) as is_actrmb, 
1 as dau, (case when rn = 1 then 1 else null end) as wau, 
rn
from daily_info a
left join act_info b 
on a.role_id = b.role_id and a.act_tag = b.act_tag;
###
