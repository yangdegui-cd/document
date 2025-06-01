/*
* @Author: dingyelen
* @Date:   2024-12-18 14:06:50
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-05-19 13:21:43
*/

###
with act_tag as(
select date, start_date, end_date, 
concat(date_format(start_date, '%Y%m%d'), '-', date_format(end_date, '%Y%m%d'), '-', cast(date_diff('day', start_date, end_date)+1 as varchar)) as act_tag, 
date_diff('day', start_date, end_date) + 1 as act_days, act_name
from hive.dow_jpnew_w.dim_gserver_activity_act
cross join unnest(sequence(start_date, end_date, interval '1' day)) as t(date)
where start_date >= date_add('month', -6, current_date)
and code = 'anisummon'
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
left join hive.dow_jpnew_w.dim_gserver_base_roleid z
on a.role_id = z.role_id 
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and z.role_id is null
), 

payment_agg as(
select part_date, date(part_date) as date, role_id, 
sum(coalesce(rawmoney, 0) * 0.052102 + coalesce(sincetime_money, 0)) as act_rmb
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and payment_item_id in ('gold_60031', 'gold_60032', 'gold_60033', 'gold_60034', 'gold_60035', 'gold_60036', 'gold_60037', 'gold_60038', 'gold_60039', 'gold_60040', 'gold_60041', 'gold_60042', 'gold_60043', 'gold_60044', 'gold_60045', 'gold_60046', 'gold_60047', 'gold_60048', 'gold_60049', 'gold_60050', 'gold_60051', 'gold_60052', 'gold_60053', 'gold_60054', 'gold_60055')
group by 1, 2, 3
), 

item_agg as(
select part_date, date(part_date) as date, role_id, 
sum(case when event_type = 'cost' then item_num else null end) as item_cost, 
sum(case when event_type = 'gain' then item_num else null end) as item_add, 
sum(case when event_type = 'cost' then item_num else null end)/2 as summon_count
from hive.dow_jpnew_r.dwd_gserver_itemchange_live
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id in ('340092', '320380', '340066', '340121')
and reason != '638'
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
b.act_rmb, c.item_cost, c.item_add, c.summon_count, 
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
a.act_rmb, a.item_cost, a.item_add, a.summon_count, 
(case when b.act_summoncount >= 460 then '[460, ∞)'
when b.act_summoncount >= 360 then '[360,460)'
when b.act_summoncount >= 260 then '[260,360)'
when b.act_summoncount >= 160 then '[160,260)'
when b.act_summoncount >= 20 then '[20,160)'
when b.act_summoncount > 0 then '[1,20)'
else '[0]' end) as item_tag, 
(case when rn = 1 and b.act_summoncount > 0 then 1 else null end) as summon_wau, 
(case when rn = 1 and b.act_moneyrmb > 0 then 1 else null end) as actrmb_wau, 
1 as dau, (case when rn = 1 then 1 else null end) as wau
from daily_info a
left join act_info b 
on a.role_id = b.role_id and a.act_tag = b.act_tag;
###