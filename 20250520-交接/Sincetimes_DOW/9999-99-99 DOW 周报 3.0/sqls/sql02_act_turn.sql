/*
* @Author: dingyelen
* @Date:   2024-12-19 14:21:59
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-02-17 11:40:29
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

item_select as(
select part_date, date(part_date) as date, 
d.act_tag, '道具获得' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, reason_id as dis_type, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join act_tag d
on date(a.part_date) = d.date
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id = '11437'
and event_type = 'gain'
and b.is_test is null
), 

paymentdetail_log as(
select part_date, date(part_date) as date, 
d.act_tag, coalesce(c.pay_tag, '未活跃') as pay_tag, 
a.role_id, a.payment_item_id as payment_itemid, b.payment_act, b.payment_name, 
(rawmoney * 0.052102 + sincetime_money) as money_rmb
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live a
left join hive.dow_jpnew_w.dim_gserver_payment_paymentitemid b
on a.payment_item_id = b.payment_itemid
left join month_tag c
on a.role_id = c.role_id
left join act_tag d
on date(a.part_date) = d.date
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and payment_item_id in ('gold_350', 'gold_351', 'gold_352', 'gold_353')
and c.is_test is null
), 

payment_select1 as(
select part_date, date, act_tag, 
'直购礼包' as type, pay_tag, role_id, payment_name as dis_type, money_rmb as target_sum 
from paymentdetail_log
), 

payment_select2 as(
select part_date, date, act_tag, 
'直购礼包上月标签' as type, pay_tag, role_id, pay_tag as dis_type, money_rmb as target_sum 
from paymentdetail_log
), 

superturntablechoice_log as(
select part_date, date(part_date) as date, 
d.act_tag, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, reward_detail1, reward_detail2, reward_detail3, reward_detail4
from hive.dow_jpnew_r.dwd_gserver_superturntablechoice_live a
left join month_tag b
on a.role_id = b.role_id
left join act_tag d
on date(a.part_date) = d.date
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and b.is_test is null
), 

superturntablechoice_explore1 as(
select part_date, date, act_tag, 
pay_tag, role_id, 
'1_转盘心愿' as reward_type, reward_detail1, 
json_extract_scalar(t.item_info, '$.reward_id') as item_id, 
json_extract_scalar(t.item_info, '$.reward_count') as item_count
from superturntablechoice_log, unnest(reward_detail1) as t(item_info)
), 

superturntablechoice_explore2 as(
select part_date, date, act_tag, 
pay_tag, role_id, 
'2_转盘心愿' as reward_type, reward_detail1, 
json_extract_scalar(t.item_info, '$.reward_id') as item_id, 
json_extract_scalar(t.item_info, '$.reward_count') as item_count
from superturntablechoice_log, unnest(reward_detail2) as t(item_info)
), 

superturntablechoice_explore3 as(
select part_date, date, act_tag, 
pay_tag, role_id, 
'3_转盘心愿' as reward_type, reward_detail1, 
json_extract_scalar(t.item_info, '$.reward_id') as item_id, 
json_extract_scalar(t.item_info, '$.reward_count') as item_count
from superturntablechoice_log, unnest(reward_detail3) as t(item_info)
), 

superturntablechoice_explore4 as(
select part_date, date, act_tag, 
pay_tag, role_id, 
'4_转盘心愿' as reward_type, reward_detail1, 
json_extract_scalar(t.item_info, '$.reward_id') as item_id, 
json_extract_scalar(t.item_info, '$.reward_count') as item_count
from superturntablechoice_log, unnest(reward_detail4) as t(item_info)
), 

superturntablechoice_explore as(
select * from superturntablechoice_explore1
union all
select * from superturntablechoice_explore2
union all
select * from superturntablechoice_explore3
union all
select * from superturntablechoice_explore4
), 

superturntablechoice_select as(
select part_date, date, act_tag, 
reward_type as type, pay_tag, role_id, b.item_name as dis_type, cast(item_count as bigint) as target_sum 
from superturntablechoice_explore a
left join hive.dow_jpnew_w.dim_gserver_additem_itemid b
on a.item_id = b.item_id
), 

event_union as(
select * from (
select * from item_select
union all
select * from payment_select1
union all
select * from payment_select2
union all
select * from superturntablechoice_select)
where act_tag != 'unknown'
)

select act_tag, pay_tag, type, dis_type, 
count(distinct role_id) as users, 
sum(target_sum) as target_sum, 
count(*) as target_count
from event_union
group by 1, 2, 3, 4;
###
