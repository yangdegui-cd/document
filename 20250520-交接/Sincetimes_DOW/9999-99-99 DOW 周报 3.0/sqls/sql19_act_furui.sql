/*
* @Author: dingyelen
* @Date:   2024-12-18 14:06:50
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-02-17 11:43:39
*/

###
with act_tag as(
select date, start_date, end_date, 
concat(date_format(start_date, '%Y%m%d'), '-', date_format(end_date, '%Y%m%d'), '-', cast(date_diff('day', start_date, end_date)+1 as varchar)) as act_tag, 
date_diff('day', start_date, end_date) + 1 as act_days, act_name
from hive.dow_jpnew_w.dim_gserver_activity_act
cross join unnest(sequence(start_date, end_date, interval '1' day)) as t(date)
where start_date >= date_add('month', -6, current_date)
and code = 'furui'
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
on date(a.part_date) = d.date
left join hive.dow_jpnew_w.dws_user_info_di z
on a.role_id = z.role_id 
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and a.is_test is null
and d.act_tag != 'unknown'
), 

payment_agg as(
select part_date, date(part_date) as date, role_id, 
sum(case when payment_item_id in('gold_60056', 'gold_60057', 'gold_60058', 'gold_60059', 'gold_60060') then coalesce(rawmoney, 0) * 0.052102 + coalesce(sincetime_money, 0) else null end) as act_rmb, 
sum(case when payment_item_id in('gold_415', 'gold_416') then coalesce(rawmoney, 0) * 0.052102 + coalesce(sincetime_money, 0) else null end) as bp_rmb
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
group by 1, 2, 3
), 

item_agg as(
select part_date, date(part_date) as date, role_id, 
sum(case when event_type = 'cost' then item_num else null end) as item_cost, 
sum(case when event_type = 'gain' then item_num else null end) as item_add
from hive.dow_jpnew_r.dwd_gserver_itemchange_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id = '340081'
group by 1, 2, 3
), 

-- corechange_agg as(
-- select part_date, date(part_date) as date, role_id, 
-- sum(coalesce(free_num, 0) + coalesce(paid_num, 0)) as core_cost_316
-- from hive.dow_jpnew_r.dwd_gserver_corechange_live
-- where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
-- and reason = '316'
-- and event_type = 'cost'
-- group by 1, 2, 3
-- ), 

summon_agg as(
select part_date, date(part_date) as date, role_id, 
sum(count) as summon_count, 
sum(case when recruitid = 21602 then count else null end) as summon_21602, 
sum(case when recruitid = 21603 then count else null end) as summon_21603
from hive.dow_jpnew_r.dwd_gserver_recruitcard_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and type = 7
group by 1, 2, 3
), 

daily_info as(
select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
b.act_rmb, b.bp_rmb, c.item_cost, c.item_add, 
e.summon_count, e.summon_21602, e.summon_21603, 
row_number() over(partition by a.role_id, a.act_tag order by a.date) as rn
from user_daily a
left join payment_agg b
on a.role_id = b.role_id and a.date = b.date
left join item_agg c
on a.role_id = c.role_id and a.date = c.date
--left join corechange_agg d
--on a.role_id = d.role_id and a.date = d.date
left join summon_agg e
on a.role_id = e.role_id and a.date = e.date
where a.act_tag != 'unknown'
), 

act_info as(
select act_tag, role_id, 
sum(act_rmb) as act_moneyrmb, 
sum(bp_rmb) as bp_rmb, 
sum(summon_count) as act_summoncount, 
sum(summon_21602) as act_summon21602, 
sum(summon_21603) as act_summon21603
from daily_info
group by 1, 2
)

select a.date, a.act_tag, 
a.role_id, a.vip_level, a.level, 
a.pay_tag, a.is_pay, a.money, a.web_money, 
a.money_rmb, a.webmoney_rmb, 
a.core_add, a.core_cost, a.core_end, 
a.sincetimes_add, a.sincetimes_cost, a.sincetimes_end, 
a.act_rmb, a.bp_rmb, a.item_cost, a.item_add, 
a.summon_count, a.summon_21602, a.summon_21603, 
(case when b.act_summoncount >= 250 then '[250, ∞)'
when b.act_summoncount >= 200 then '[200,250)'
when b.act_summoncount >= 150 then '[150,200)'
when b.act_summoncount >= 80 then '[80,150)'
when b.act_summoncount >= 60 then '[60,80)'
when b.act_summoncount >= 20 then '[20,60)'
when b.act_summoncount > 0 then '[1,20)'
else '[0]' end) as item_tag, 
(case when rn = 1 and b.act_summoncount > 0 then 1 else null end) as summon_wau, 
(case when rn = 1 and b.act_summon21602 > 0 then 1 else null end) as summon21602_wau, 
(case when rn = 1 and b.act_summon21603 > 0 then 1 else null end) as summon21603_wau, 
(case when rn = 1 and b.act_moneyrmb > 0 then 1 else null end) as actrmb_wau, 
(case when rn = 1 and b.bp_rmb > 0 then 1 else null end) as bprmb_wau, 
1 as dau, (case when rn = 1 then 1 else null end) as wau
from daily_info a
left join act_info b 
on a.role_id = b.role_id and a.act_tag = b.act_tag;
###
