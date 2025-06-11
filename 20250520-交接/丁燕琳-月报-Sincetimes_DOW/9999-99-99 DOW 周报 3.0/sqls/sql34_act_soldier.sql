/*
* @Author: dingyelen
* @Date:   2024-12-18 14:06:50
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-03-04 11:22:00
*/

###
with act_tag as(
select date, start_date, end_date, 
concat(date_format(start_date, '%Y%m%d'), '-', date_format(end_date, '%Y%m%d'), '-', cast(date_diff('day', start_date, end_date)+1 as varchar)) as act_tag, 
date_diff('day', start_date, end_date) + 1 as act_days, act_name
from hive.dow_jpnew_w.dim_gserver_activity_act
cross join unnest(sequence(start_date, end_date, interval '1' day)) as t(date)
where start_date >= date_add('month', -6, current_date)
and code = 'soldier'
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
sum(coalesce(rawmoney, 0) * 0.052102 + coalesce(sincetime_money, 0)) as act_rmb
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and payment_item_id in ('gold_305', 'gold_306', 'gold_307')
group by 1, 2, 3
), 

item_agg as(
select part_date, date(part_date) as date, role_id, 
sum(case when event_type = 'cost' then item_num else null end) as summon_itemcost, 
sum(case when event_type = 'gain' then item_num else null end) as summon_itemadd
from hive.dow_jpnew_r.dwd_gserver_itemchange_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id = '28'
group by 1, 2, 3
), 

-- summon_agg as(
-- select part_date, date(part_date) as date, role_id, 
-- sum(count) as summon_count
-- from hive.dow_jpnew_r.dwd_gserver_recruitcard_live
-- where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
-- and recruitid = 19303
-- group by 1, 2, 3
-- ), 

daily_info as(
select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
b.act_rmb, c.summon_itemcost, c.summon_itemadd, c.summon_itemcost/200 as summon_count, 
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
sum(summon_count) as act_summoncount
from daily_info
group by 1, 2
)

select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
a.act_rmb, a.summon_itemcost, a.summon_itemadd, a.summon_count, 
(case when a.summon_count>=50 then 1 else null end) as is_dailysummongt50, 
(case when b.act_summoncount >= 150 then '[150, ∞)'
when b.act_summoncount >= 100 then '[100,150)'
when b.act_summoncount >= 50 then '[50,100)'
when b.act_summoncount > 0 then '[1,50)'
else '[0]' end) as item_tag, 
(case when rn = 1 and b.act_summoncount > 0 then 1 else null end) as summon_wau, 
(case when rn = 1 and b.act_moneyrmb > 0 then 1 else null end) as actrmb_wau, 
1 as dau, (case when rn = 1 then 1 else null end) as wau, rn
from daily_info a
left join act_info b 
on a.role_id = b.role_id and a.act_tag = b.act_tag;
###
