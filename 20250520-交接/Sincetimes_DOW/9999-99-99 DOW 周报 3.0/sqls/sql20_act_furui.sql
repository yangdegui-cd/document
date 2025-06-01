/*
* @Author: dingyelen
* @Date:   2024-12-19 14:21:59
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-02-17 11:43:48
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

item_select as(
select part_date, date(part_date) as date, d.act_tag, 
'抽卡道具渠道' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, reason_cn as dis_type1, null as dis_type2, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_addgold_reason c
on a.reason_id = c.id
left join act_tag d
on date(a.part_date) = d.date
where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id = '340081'
and event_type = 'gain'
and b.is_test is null
and d.act_tag != 'unknown'
), 

-- item_select2 as(
-- select part_date, date(part_date) as date, d.act_tag, 
-- '抽奖每日渠道' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
-- a.role_id, reason_cn as dis_type1, part_date as dis_type2, item_num as target_sum
-- from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
-- left join month_tag b
-- on a.role_id = b.role_id
-- left join hive.dow_jpnew_w.dim_gserver_addgold_reason c
-- on a.reason_id = c.id
-- left join act_tag d
-- on date(a.part_date) = d.date
-- where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
-- and item_id = '360044'
-- and event_type = 'gain'
-- and b.is_test is null
-- and d.act_tag != 'unknown'
-- ), 

-- actshopping_select as(
-- select part_date, date(part_date) as date, d.act_tag, 
-- '兑换信物金币消耗' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
-- a.role_id, coalesce(b.pay_tag, '未活跃') as dis_type1, null as dis_type2, currency_num as target_sum
-- from hive.dow_jpnew_r.dwd_gserver_actshopping_live a
-- left join month_tag b
-- on a.role_id = b.role_id
-- left join act_tag d
-- on date(a.part_date) = d.date
-- where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
-- and act_id = '19403'
-- and b.is_test is null
-- and d.act_tag != 'unknown'
-- ),

-- randomrewardboxbig_log as(
-- select part_date, date(part_date) as date, event_time, 
-- (case when a.part_date between $last_startdate and $last_enddate then '上期'
-- when a.part_date between $start_date and $end_date then '当期' else 'unknown' end) as act_tag, 
-- coalesce(b.pay_tag, '未活跃') as pay_tag, a.role_id, layer_id, reward_detail
-- from hive.dow_jpnew_r.dwd_gserver_randomrewardboxbig_live a
-- left join month_tag b
-- on a.role_id = b.role_id
-- where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
-- and b.is_test is null
-- ), 

-- randomrewardboxbig_explore as(
-- select part_date, date, act_tag, 
-- pay_tag, role_id, layer_id, reward_detail, 
-- json_extract_scalar(t.item_info, '$.reward_id') as item_id, 
-- cast(json_extract_scalar(t.item_info, '$.reward_count') as bigint) as item_count
-- from randomrewardboxbig_log, unnest(reward_detail) as t(item_info)
-- ), 

-- randomrewardboxbig_select1 as(
-- select part_date, date, act_tag, 
-- 'randomrewardboxbig' as type, pay_tag, 
-- role_id, cast(layer_id as varchar) as dis_type1, b.item_name as dis_type2, item_count as target_sum 
-- from randomrewardboxbig_explore a
-- left join hive.dow_jpnew_w.dim_gserver_additem_itemid b
-- on a.item_id = b.item_id
-- ), 

-- randomrewardboxbig_rank as(
-- select part_date, date, act_tag, 
-- pay_tag, role_id, layer_id, reward_detail, 
-- row_number() over(partition by role_id, act_tag order by event_time desc) as rn_desc
-- from randomrewardboxbig_log
-- ), 

-- randomrewardboxbig_select2 as(
-- select part_date, date, act_tag, 
-- '最后停止层' as type, pay_tag, 
-- role_id, cast(layer_id as varchar) as dis_type1, null as dis_type2, null as target_sum 
-- from randomrewardboxbig_rank
-- where rn_desc = 1
-- ), 

-- pprreward_select as(
-- select part_date, date(part_date) as date, d.act_tag, 
-- 'pprid_reward_type' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
-- a.role_id, concat(cast(pprid as varchar), '_', rewardtype) as dis_type1, c.item_name as dis_type2, itemcount as target_sum
-- from hive.dow_jpnew_r.dwd_gserver_pprreward_live a
-- left join month_tag b
-- on a.role_id = b.role_id
-- left join hive.dow_jpnew_w.dim_gserver_additem_itemid c
-- on cast(a.itemid as varchar) = c.item_id
-- left join act_tag d
-- on date(a.part_date) = d.date
-- where a.part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
-- and pprid in (160, 164)
-- and b.is_test is null
-- and d.act_tag != 'unknown'
-- ), 

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
and c.payment_act = '福瑞赏'
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
select * from item_select
union all
select * from paymentdetail_select1
union all
select * from paymentdetail_select2
-- union all
-- -- select * from randomrewardboxbig_select1
-- -- union all
-- -- select * from randomrewardboxbig_select2
-- -- union all
-- select * from actshopping_select
-- union all
-- select * from pprreward_select
)
where act_tag != 'unknown'
)

select act_tag, pay_tag, type, dis_type1, dis_type2, 
count(distinct role_id) as users, 
sum(target_sum) as target_sum, 
count(*) as target_count
from event_union
group by 1, 2, 3, 4, 5;
###
