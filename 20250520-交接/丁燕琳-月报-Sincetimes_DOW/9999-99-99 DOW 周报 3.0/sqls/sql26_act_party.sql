/*
* @Author: dingyelen
* @Date:   2024-12-19 14:21:59
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-02-17 15:46:28
*/

###
with act_tag as(
select date, start_date, end_date, 
concat(date_format(start_date, '%Y%m%d'), '-', date_format(end_date, '%Y%m%d'), '-', cast(date_diff('day', start_date, end_date)+1 as varchar)) as act_tag, 
date_diff('day', start_date, end_date) + 1 as act_days, act_name
from hive.dow_jpnew_w.dim_gserver_activity_act
cross join unnest(sequence(start_date, end_date, interval '1' day)) as t(date)
where start_date >= date_add('month', -6, current_date)
and code = 'party'
), 

month_tag as(
select role_id, month, is_test, pay_tag
from hive.dow_jpnew_w.dws_user_info_mi
where month = date $pay_month
), 

item_select1 as(
select part_date, date(part_date) as date, d.act_tag, 
'抽奖渠道' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, reason_cn as dis_type1, null as dis_type2, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_addgold_reason c
on a.reason_id = c.id
left join act_tag d
on date(a.part_date) = d.date
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id = '11483'
and event_type = 'gain'
and b.is_test is null
and d.act_tag != 'unknown'
), 

item_select2 as(
select part_date, date(part_date) as date, d.act_tag, 
'抽奖次数' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, e.item_name as dis_type1, null as dis_type2, item_num/2 as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_addgold_reason c
on a.reason_id = c.id
left join act_tag d
on date(a.part_date) = d.date
left join hive.dow_jpnew_w.dim_gserver_additem_itemid e
on a.item_id = e.item_id
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and a.item_id = '11483'
and event_type = 'cost'
and b.is_test is null
and d.act_tag != 'unknown'
), 

item_select3 as(
select part_date, date(part_date) as date, d.act_tag, 
'御守彩珠消耗' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, e.item_name as dis_type1, null as dis_type2, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_addgold_reason c
on a.reason_id = c.id
left join act_tag d
on date(a.part_date) = d.date
left join hive.dow_jpnew_w.dim_gserver_additem_itemid e
on a.item_id = e.item_id
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and a.item_id in ('11483', '11466')
and event_type = 'cost'
and b.is_test is null
and d.act_tag != 'unknown'
), 

item_select4 as(
select part_date, date(part_date) as date, d.act_tag, 
'御守每日消耗' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, part_date as dis_type1, null as dis_type2, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_addgold_reason c
on a.reason_id = c.id
left join act_tag d
on date(a.part_date) = d.date
left join hive.dow_jpnew_w.dim_gserver_additem_itemid e
on a.item_id = e.item_id
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and a.item_id = '11466'
and event_type = 'cost'
and b.is_test is null
and d.act_tag != 'unknown'
), 

gold_select as(
select part_date, date(part_date) as date, d.act_tag, 
'兑换彩珠金币消耗' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, pay_tag as dis_type1, null as dis_type2, coalesce(free_num, 0) + coalesce(paid_num, 0) as target_sum
from hive.dow_jpnew_r.dwd_gserver_corechange_live a
left join month_tag b
on a.role_id = b.role_id
left join act_tag d
on date(a.part_date) = d.date
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and reason = '316'
and event_type = 'cost'
and b.is_test is null
and d.act_tag != 'unknown'
), 

actshopping_select as(
select part_date, date(part_date) as date, d.act_tag, 
'商店兑换' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, c.item_name as dis_type1, null as dis_type2, currency_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_actshopping_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_additem_itemid c
on a.item_id = c.item_id
left join act_tag d
on date(a.part_date) = d.date
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and act_id = '18804'
and b.is_test is null
and d.act_tag != 'unknown'
),

pprreward_select as(
select part_date, date(part_date) as date, d.act_tag, 
'pprid_reward_type' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, concat(cast(pprid as varchar), '_', rewardtype) as dis_type1, c.item_name as dis_type2, itemcount as target_sum
from hive.dow_jpnew_r.dwd_gserver_pprreward_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_additem_itemid c
on cast(a.itemid as varchar) = c.item_id
left join act_tag d
on date(a.part_date) = d.date
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and pprid in (12, 16)
and b.is_test is null
and d.act_tag != 'unknown'
), 

payment_log as(
select part_date, date(part_date) as date, d.act_tag, 
coalesce(b.pay_tag, '未活跃') as pay_tag, a.role_id, 
payment_item_id as payment_itemid, c.payment_act, c.payment_name, 
coalesce(rawmoney * 0.052102, 0) + coalesce(sincetime_money, 0) as money_rmb 
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_payment_paymentitemid c
on a.payment_item_id = c.payment_itemid
left join act_tag d
on date(a.part_date) = d.date
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and payment_item_id in ('gold_371', 'gold_372', 'gold_373', 'gold_374', 'gold_375', 'gold_505', 'gold_506')
-- and c.payment_act = '行脚商人'
and b.is_test is null
), 

paymentdetail_select1 as(
select part_date, date, act_tag, 
'直购礼包礼包聚合1' as type, pay_tag, 
role_id, payment_name as dis_type1, null as dis_type2, money_rmb as target_sum
from payment_log
), 

paymentdetail_select2 as(
select part_date, date, act_tag, 
'直购礼包礼包聚合2' as type, pay_tag, 
role_id, pay_tag as dis_type1, null as dis_type2, money_rmb as target_sum
from payment_log
), 

event_union as(
select * from (
select * from item_select1
union all
select * from item_select2
union all
select * from item_select3
union all
select * from item_select4
union all
select * from gold_select
union all
select * from actshopping_select
union all
select * from pprreward_select
union all
select * from paymentdetail_select1
union all
select * from paymentdetail_select2
)
where act_tag != 'unknown'
)

select act_tag, pay_tag, type, dis_type1, dis_type2, 
count(distinct role_id) as users, 
sum(target_sum) as target_sum, 
count(*) as target_count
from event_union
group by 1, 2, 3, 4, 5
###
