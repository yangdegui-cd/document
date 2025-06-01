/*
* @Author: dingyelen
* @Date:   2024-12-19 14:21:59
* @Last Modified by:   dingyelen
* @Last Modified time: 2025-05-19 13:21:38
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

item_select1 as(
select part_date, date(part_date) as date, d.act_tag, 
'道具获得1' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, reason_cn as dis_type1, null as dis_type2, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_addgold_reason c
on a.reason_id = c.id
left join act_tag d
on date(a.part_date) = d.date
left join hive.dow_jpnew_w.dim_gserver_base_roleid z
on a.role_id = z.role_id 
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and item_id in ('340092', '320380', '340066', '340121')
and event_type = 'gain'
and z.role_id is null
), 

item_select2 as(
select part_date, date(part_date) as date, d.act_tag, 
'抽卡商品' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, c.item_name as dis_type1, null as dis_type2, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_additem_itemid c
on a.item_id = c.item_id
left join act_tag d
on date(a.part_date) = d.date
left join hive.dow_jpnew_w.dim_gserver_base_roleid z
on a.role_id = z.role_id 
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and a.reason_subid = '17014'
and event_type = 'gain'
and z.role_id is null
), 

item_select3 as(
select part_date, date(part_date) as date, d.act_tag, 
'卡池每日人均抽卡' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, c.summon_cn as dis_type1, part_date as dis_type2, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_recruitcard_recruitid c
on a.reason_subid = c.summon_id
left join act_tag d
on date(a.part_date) = d.date
left join hive.dow_jpnew_w.dim_gserver_base_roleid z
on a.role_id = z.role_id 
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and event_type = 'gain'
and item_id in ('360055', '329955', '329954', '329953', '329939', '329938', '329880', '329879', '16151', '13290', '13289', '13288', '13287', '13283', '13282', '13280', '13279', '13278', '13274', '13272', '13265', '13264', '13263', '13259', '13257', '11535', '11528', '11527', '11526', '11514', '11513', '11489', '10978', '10553', '10462', '10461', '10055', '10030', '3')
and reason_subid in ('17019', '17020', '17021', '17022', '17023', '17024', '17025', '17026', '17027', '17028', '17039')
and z.role_id is null
), 

item_select4 as(
select part_date, date(part_date) as date, d.act_tag, 
'卡池人均抽卡' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, c.summon_cn as dis_type1, null as dis_type2, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_recruitcard_recruitid c
on a.reason_subid = c.summon_id
left join act_tag d
on date(a.part_date) = d.date
left join hive.dow_jpnew_w.dim_gserver_base_roleid z
on a.role_id = z.role_id 
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and event_type = 'gain'
and item_id in ('360055', '329955', '329954', '329953', '329939', '329938', '329880', '329879', '16151', '13290', '13289', '13288', '13287', '13283', '13282', '13280', '13279', '13278', '13274', '13272', '13265', '13264', '13263', '13259', '13257', '11535', '11528', '11527', '11526', '11514', '11513', '11489', '10978', '10553', '10462', '10461', '10055', '10030', '3')
and reason_subid in ('17019', '17020', '17021', '17022', '17023', '17024', '17025', '17026', '17027', '17028', '17039')
and z.role_id is null
), 

item_select5 as(
select part_date, date(part_date) as date, d.act_tag, 
'抽卡掉落' as type, coalesce(b.pay_tag, '未活跃') as pay_tag, 
a.role_id, c.item_name as dis_type1, null as dis_type2, item_num as target_sum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join month_tag b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_additem_itemid c
on a.item_id = c.item_id
left join act_tag d
on date(a.part_date) = d.date
left join hive.dow_jpnew_w.dim_gserver_base_roleid z
on a.role_id = z.role_id 
where part_date >= date_format(date_add('month', -6, current_date), '%Y-%m-%d')
and event_type = 'gain'
and reason = '905'
and z.role_id is null
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
left join hive.dow_jpnew_w.dim_gserver_base_roleid z
on a.role_id = z.role_id 
where payment_item_id in('gold_60031', 'gold_60032', 'gold_60033', 'gold_60034', 'gold_60035', 'gold_60036', 'gold_60037', 'gold_60038', 'gold_60039', 'gold_60040', 'gold_60041', 'gold_60042', 'gold_60043', 'gold_60044', 'gold_60045', 'gold_60046', 'gold_60047', 'gold_60048', 'gold_60049', 'gold_60050', 'gold_60051', 'gold_60052', 'gold_60053', 'gold_60054', 'gold_60055')
and z.role_id is null
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
select * from item_select5
union all
select * from paymentdetail_select1
union all
select * from paymentdetail_select2)
where act_tag != 'unknown'
)

select act_tag, pay_tag, type, dis_type1, dis_type2, 
count(distinct role_id) as users, 
sum(target_sum) as target_sum, 
count(*) as target_count
from event_union
group by 1, 2, 3, 4, 5;
###
