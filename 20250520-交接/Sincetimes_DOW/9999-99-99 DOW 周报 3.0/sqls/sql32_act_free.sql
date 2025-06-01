/*
* @Author: dingyelen
* @Date:   2024-12-18 14:06:50
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-03-04 11:55:20
*/

###
with act_tag as(
select date, start_date, end_date, 
concat(date_format(start_date, '%Y%m%d'), '-', date_format(end_date, '%Y%m%d'), '-', cast(date_diff('day', start_date, end_date)+1 as varchar)) as act_tag, 
date_diff('day', start_date, end_date) + 1 as act_days, act_name
from hive.dow_jpnew_w.dim_gserver_activity_act
cross join unnest(sequence(start_date, end_date, interval '1' day)) as t(date)
where start_date >= date_add('month', -6, current_date)
and code = 'free'
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
left join hive.dow_jpnew_w.dim_gserver_payment_paymentitemid b
on a.payment_item_id = b.payment_itemid
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
-- and b.payment_act = '团购礼包'
and a.payment_item_id in ('gold_386', 'gold_387', 'gold_388', 'gold_389')
group by 1, 2, 3
), 

corechange_agg as(
select part_date, date(part_date) as date, role_id, 
count(*) as act_count, 
sum(case when coalesce(free_num, 0) + coalesce(paid_num, 0) = 1200 then 1 else null end) as heishi_1200, 
sum(case when coalesce(free_num, 0) + coalesce(paid_num, 0) = 3000 then 1 else null end) as heishi_3000, 
sum(case when coalesce(free_num, 0) + coalesce(paid_num, 0) = 4200 then 1 else null end) as heishi_4200, 
sum(case when coalesce(free_num, 0) + coalesce(paid_num, 0) = 4800 then 1 else null end) as heishi_4800, 
sum(case when coalesce(free_num, 0) + coalesce(paid_num, 0) = 5600 then 1 else null end) as heishi_5600, 
sum(case when coalesce(free_num, 0) + coalesce(paid_num, 0) = 6600 then 1 else null end) as heishi_6600, 
sum(case when coalesce(free_num, 0) + coalesce(paid_num, 0) = 8600 then 1 else null end) as heishi_8600, 
sum(case when coalesce(free_num, 0) + coalesce(paid_num, 0) = 11600 then 1 else null end) as heishi_11600
from hive.dow_jpnew_r.dwd_gserver_corechange_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and reason = '659'
and event_type = 'cost'
group by 1, 2, 3
), 

item_agg as(
select part_date, date(part_date) as date, role_id, 
sum(case when event_type = 'cost' then item_num else null end) as item_cost, 
sum(case when event_type = 'gain' then item_num else null end) as item_add
from hive.dow_jpnew_r.dwd_gserver_itemchange_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id = '11431'
group by 1, 2, 3
), 

daily_info as(
select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
b.act_rmb, b.act_paycount, d.item_cost, d.item_add, d.item_cost as summon_count, 
c.act_count, c.heishi_1200, c.heishi_3000, c.heishi_4200, 
c.heishi_4800, c.heishi_5600, c.heishi_6600, c.heishi_8600, c.heishi_11600, 
row_number() over(partition by a.role_id, a.act_tag order by a.date) as rn
from user_daily a
left join payment_agg b
on a.role_id = b.role_id and a.date = b.date
left join corechange_agg c
on a.role_id = c.role_id and a.date = c.date
left join item_agg d
on a.role_id = d.role_id and a.date = d.date
where a.act_tag != 'unknown'
), 

act_info as(
select act_tag, role_id, 
sum(money_rmb) as money_rmb,  
sum(act_rmb) as act_moneyrmb, 
sum(summon_count) as summon_count
from daily_info
group by 1, 2
)

select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
a.act_rmb, a.act_paycount, a.item_cost, a.item_add, a.summon_count, 
a.act_count, a.heishi_1200, a.heishi_3000, a.heishi_4200, 
a.heishi_4800, a.heishi_5600, a.heishi_6600, a.heishi_8600, a.heishi_11600, 
(case when b.summon_count >= 450 then '[450, ∞)'
when b.summon_count >= 350 then '[350,450)'
when b.summon_count >= 250 then '[250,350)'
when b.summon_count >= 150 then '[150,250)'
when b.summon_count >= 100 then '[100,150)'
when b.summon_count >= 50 then '[50,100)'
when b.summon_count > 0 then '[1,50)'
else '[0]' end) as item_tag, 
(case when rn = 1 and b.summon_count > 0 then 1 else null end) as summon_wau, 
(case when rn = 1 and b.act_moneyrmb > 0 then 1 else null end) as actrmb_wau, 
(case when rn = 1 and b.money_rmb > 0 then 1 else null end) as pay_wau, 
1 as dau, 
(case when a.act_rmb > 0 then 1 else null end) as is_actrmb, 
(case when rn = 1 then 1 else null end) as wau
from daily_info a
left join act_info b 
on a.role_id = b.role_id and a.act_tag = b.act_tag;
###
