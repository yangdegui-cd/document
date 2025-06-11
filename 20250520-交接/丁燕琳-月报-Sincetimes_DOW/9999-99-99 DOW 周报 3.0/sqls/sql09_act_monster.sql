/*
* @Author: dingyelen
* @Date:   2024-12-19 16:09:18
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-02-17 11:41:36
*/

###
with act_tag as(
select date, start_date, end_date, 
concat(date_format(start_date, '%Y%m%d'), '-', date_format(end_date, '%Y%m%d'), '-', cast(date_diff('day', start_date, end_date)+1 as varchar)) as act_tag, 
date_diff('day', start_date, end_date) + 1 as act_days, act_name
from hive.dow_jpnew_w.dim_gserver_activity_act
cross join unnest(sequence(start_date, end_date, interval '1' day)) as t(date)
where start_date >= date_add('month', -6, current_date)
and code = 'monster'
), 

month_tag as(
select role_id, month, is_test, pay_tag
from hive.dow_jpnew_w.dws_user_info_mi
where month = date $pay_month
), 

summon_log as(
select part_date, date(part_date) as date, event_time, 
d.act_tag, a.role_id, cast(recruitid as varchar) as summon_id, count as summon_count, 
rewardids as item_infos
from hive.dow_jpnew_r.dwd_gserver_recruitcard_live a
left join month_tag b
on a.role_id = b.role_id
left join act_tag d
on date(a.part_date) = d.date
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and b.is_test is null
and d.act_tag != 'unknown'
and recruitid = 19303
), 

summon_agg as(
select act_tag, summon_id, role_id, 
sum(summon_count) as summon_count
from summon_log 
group by 1, 2, 3
), 

summon_reward_unnest as(
select part_date, date, event_time, 
act_tag, role_id, summon_id, summon_count, 
item_info, 
json_extract_scalar(t.item_info, '$.itemid') as item_id, 
cast(json_extract_scalar(t.item_info, '$.itemcount') as bigint) as item_count
from summon_log, unnest(item_infos) as t(item_info)
), 

summon_reward_log as(
select a.part_date, a.date, a.event_time, 
a.act_tag, a.role_id, a.summon_id, a.summon_count, 
a.item_info, a.item_id, a.item_count, d.item_name, 
cast((case when c.shard_id = a.item_id and a.item_count = 40 then cast(a.item_id as bigint) - 12000 else cast(a.item_id as bigint) end) as varchar) as itemid_fit
from summon_reward_unnest a 
left join hive.dow_jpnew_w.dim_gserver_levelup_heroid c
on a.item_id = c.hero_id
left join hive.dow_jpnew_w.dim_gserver_additem_itemid d
on a.item_id = d.item_id
), 

item_agg as(
select act_tag, role_id, itemid_fit, summon_id, 
sum(item_count) as item_count
from summon_reward_log
group by 1, 2, 3, 4
), 

superturntablechoice_log as(
select part_date, date(part_date) as date, event_time, 
d.act_tag, coalesce(b.pay_tag, '未活跃') as pay_tag, a.role_id, 
act_id, reward_detail1, reward_detail2, reward_detail3, reward_detail4
from hive.dow_jpnew_r.dwd_gserver_superturntablechoice_live a
left join month_tag b
on a.role_id = b.role_id
left join act_tag d
on date(a.part_date) = d.date
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and act_id = '19303'
and b.is_test is null
and d.act_tag != 'unknown'
), 

superturntablechoice_select as(
select * from(
select part_date, date, act_tag, act_id, 
pay_tag, role_id, reward_detail1, reward_detail2, reward_detail3, reward_detail4, 
row_number() over(partition by act_tag, act_id, role_id order by event_time desc) as rn_desc
from superturntablechoice_log)
where rn_desc = 1
), 

superturntablechoice_summon_info as(
select part_date, date, a.act_tag, a.act_id, 
pay_tag, a.role_id, summon_count, 
reward_detail1, reward_detail2, reward_detail3, reward_detail4
from superturntablechoice_select a
left join summon_agg b
on a.act_tag = b.act_tag
and a.act_id = b.summon_id 
and a.role_id = b.role_id
), 

superturntablechoice_explore1 as(
select part_date, date, act_tag, act_id, 
pay_tag, role_id, 
'1_转盘心愿' as reward_type, reward_detail1, summon_count, 
json_extract_scalar(t.item_info, '$.reward_id') as item_id, 
json_extract_scalar(t.item_info, '$.reward_count') as item_count
from superturntablechoice_summon_info, unnest(reward_detail1) as t(item_info)
), 

superturntablechoice_explore2 as(
select part_date, date, act_tag, act_id, 
pay_tag, role_id, 
'2_转盘心愿' as reward_type, reward_detail2, summon_count, 
json_extract_scalar(t.item_info, '$.reward_id') as item_id, 
json_extract_scalar(t.item_info, '$.reward_count') as item_count
from superturntablechoice_summon_info, unnest(reward_detail2) as t(item_info)
), 

superturntablechoice_explore3 as(
select part_date, date, act_tag, act_id, 
pay_tag, role_id, 
'3_转盘心愿' as reward_type, reward_detail3, summon_count, 
json_extract_scalar(t.item_info, '$.reward_id') as item_id, 
json_extract_scalar(t.item_info, '$.reward_count') as item_count
from superturntablechoice_summon_info, unnest(reward_detail3) as t(item_info)
), 

superturntablechoice_explore4 as(
select part_date, date, act_tag, act_id, 
pay_tag, role_id, 
'4_转盘心愿' as reward_type, reward_detail4, summon_count, 
json_extract_scalar(t.item_info, '$.reward_id') as item_id, 
json_extract_scalar(t.item_info, '$.reward_count') as item_count
from superturntablechoice_summon_info, unnest(reward_detail4) as t(item_info)
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

superturntablechoice_trans as(
select part_date, date, act_tag, act_id, 
reward_type, pay_tag, role_id, a.item_id, b.item_name, summon_count
from superturntablechoice_explore a
left join hive.dow_jpnew_w.dim_gserver_additem_itemid b
on a.item_id = b.item_id
), 

superturntablechoice_agg as(
select distinct act_tag, act_id, role_id, pay_tag, 
reward_type, item_id, item_name, summon_count
from superturntablechoice_trans
)

select a.act_tag, a.act_id, a.pay_tag, 
a.reward_type, a.item_id, a.item_name, a.role_id,  
sum(a.summon_count) as summon_count, 
sum(b.item_count) as item_count
from superturntablechoice_agg a
left join item_agg b
on a.act_tag = b.act_tag
and a.act_id = b.summon_id
and a.role_id = b.role_id
and a.item_id = b.itemid_fit
group by 1, 2, 3, 4, 5, 6, 7;
###
