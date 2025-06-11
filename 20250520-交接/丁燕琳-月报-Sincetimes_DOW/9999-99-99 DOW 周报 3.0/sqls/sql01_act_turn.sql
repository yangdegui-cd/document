/*
* @Author: dingyelen
* @Date:   2024-12-18 14:06:50
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-02-17 11:40:15
*/

###
with act_tag as(
select date, start_date, end_date, 
concat(date_format(start_date, '%Y%m%d'), '-', date_format(end_date, '%Y%m%d'), '-', cast(date_diff('day', start_date, end_date)+1 as varchar)) as act_tag, 
date_diff('day', start_date, end_date) + 1 as act_days, act_name
from hive.dow_jpnew_w.dim_gserver_activity_act
cross join unnest(sequence(start_date, end_date, interval '1' day)) as t(date)
where start_date >= date_add('month', -6, current_date)
and code = 'turn'
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
and d.act_tag is not null
), 

payment_agg as(
select part_date, date(part_date) as date, role_id, 
sum(coalesce(rawmoney, 0) * 0.052102) + sum(coalesce(sincetime_money, 0)) as act_rmb
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and payment_item_id in ('gold_350', 'gold_351', 'gold_352', 'gold_353')
group by 1, 2, 3
), 

item_agg as(
select part_date, date(part_date) as date, role_id, 
sum(case when event_type = 'cost' then item_num else null end) as item_cost, 
sum(case when event_type = 'gain' then item_num else null end) as item_add, 
sum(case when event_type = 'gain' and reason = '316' then item_num else null end) as item_add_316
from hive.dow_jpnew_r.dwd_gserver_itemchange_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id = '11437'
group by 1, 2, 3
), 

corechange_agg as(
select part_date, date(part_date) as date, role_id, 
sum(coalesce(free_num, 0) + coalesce(paid_num, 0)) as core_cost_316
from hive.dow_jpnew_r.dwd_gserver_corechange_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and reason = '316'
and event_type = 'cost'
group by 1, 2, 3
), 

summon_agg as(
select part_date, date(part_date) as date, role_id, 
sum(count) as summon_count, 
sum(case when recruitid = 18101 then count else null end) as summon_18101, 
sum(case when recruitid = 18102 then count else null end) as summon_18102, 
sum(case when recruitid = 18103 then count else null end) as summon_18103
from hive.dow_jpnew_r.dwd_gserver_recruitcard_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and recruitid in (18101, 18102, 18103)
group by 1, 2, 3
), 

daily_info as(
select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
b.act_rmb, c.item_cost, c.item_add, c.item_add_316, d.core_cost_316, 
e.summon_count, e.summon_18101, e.summon_18102, e.summon_18103, 
row_number() over(partition by a.role_id, a.act_tag order by a.date) as rn
from user_daily a
left join payment_agg b
on a.role_id = b.role_id and a.date = b.date
left join item_agg c
on a.role_id = c.role_id and a.date = c.date
left join corechange_agg d
on a.role_id = d.role_id and a.date = d.date
left join summon_agg e
on a.role_id = e.role_id and a.date = e.date
where a.act_tag != 'unknown'
), 

act_info as(
select act_tag, role_id, 
sum(act_rmb) as act_moneyrmb, 
sum(summon_count) as act_summoncount, 
sum(summon_18101) as act_summon18101, 
sum(summon_18102) as act_summon18102, 
sum(summon_18103) as act_summon18103
from daily_info
group by 1, 2
)

select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
a.act_rmb, a.item_cost, a.item_add, a.item_add_316, a.core_cost_316, 
a.summon_count, a.summon_18101, a.summon_18102, a.summon_18103, 
(case when b.act_summoncount >= 500 then '[500, ∞)'
when b.act_summoncount >= 400 then '[400,500)'
when b.act_summoncount >= 300 then '[300,400)'
when b.act_summoncount >= 240 then '[240,300)'
when b.act_summoncount >= 160 then '[160,240)'
when b.act_summoncount >= 60 then '[60,160)'
when b.act_summoncount > 0 then '(0,60)'
else '[0]' end) as item_tag, 
(case when b.act_summon18101 >= 500 then '[500, ∞)'
when b.act_summon18101 >= 400 then '[400,500)'
when b.act_summon18101 >= 300 then '[300,400)'
when b.act_summon18101 >= 240 then '[240,300)'
when b.act_summon18101 >= 160 then '[160,240)'
when b.act_summon18101 >= 60 then '[60,160)'
when b.act_summon18101 > 0 then '(0,60)'
else '[0]' end) as itemtag_18101, 
(case when b.act_summon18102 >= 500 then '[500, ∞)'
when b.act_summon18102 >= 400 then '[400,500)'
when b.act_summon18102 >= 300 then '[300,400)'
when b.act_summon18102 >= 240 then '[240,300)'
when b.act_summon18102 >= 160 then '[160,240)'
when b.act_summon18102 >= 60 then '[60,160)'
when b.act_summon18102 > 0 then '(0,60)'
else '[0]' end) as itemtag_18102, 
(case when b.act_summon18103 >= 500 then '[500, ∞)'
when b.act_summon18103 >= 400 then '[400,500)'
when b.act_summon18103 >= 300 then '[300,400)'
when b.act_summon18103 >= 240 then '[240,300)'
when b.act_summon18103 >= 160 then '[160,240)'
when b.act_summon18103 >= 60 then '[60,160)'
when b.act_summon18103 > 0 then '(0,60)'
else '[0]' end) as itemtag_18103, 
(case when a.rn = 1 then b.act_summon18101*1.0000/b.act_summoncount * b.act_moneyrmb else null end) as rmb_18101, 
(case when a.rn = 1 then b.act_summon18102*1.0000/b.act_summoncount * b.act_moneyrmb else null end) as rmb_18102, 
(case when a.rn = 1 then b.act_summon18103*1.0000/b.act_summoncount * b.act_moneyrmb else null end) as rmb_18103, 
(case when rn = 1 and b.act_summoncount > 0 then 1 else null end) as summon_wau, 
(case when rn = 1 and b.act_summon18101 > 0 then 1 else null end) as summon18101_wau, 
(case when rn = 1 and b.act_summon18102 > 0 then 1 else null end) as summon18102_wau, 
(case when rn = 1 and b.act_summon18103 > 0 then 1 else null end) as summon18103_wau, 
1 as dau, (case when rn = 1 then 1 else null end) as wau
from daily_info a
left join act_info b 
on a.role_id = b.role_id and a.act_tag = b.act_tag;
###
