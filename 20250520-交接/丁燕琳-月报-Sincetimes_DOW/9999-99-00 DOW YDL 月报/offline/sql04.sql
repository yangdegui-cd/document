with new_user_tag_info as(
select "#user_id" as uid,
max(case when cluster_name = 'da_install_ts' then tag_value_tm else null end) as da_install_ts,
max(case when cluster_name = 'da_firstpay_ts' then tag_value_tm else null end) as da_firstpay_ts,
max(case when cluster_name = 'da_lastlogin_ts' then tag_value_tm else null end) as da_lastlogin_ts,
max(case when cluster_name = 'da_lastpay_ts' then tag_value_tm else null end) as da_lastpay_ts,
max(case when cluster_name = 'da_vip_level' then tag_value_num else null end) as da_vip_level,
max(case when cluster_name = 'da_user_level' then tag_value_num else null end) as da_user_level,
max(case when cluster_name = 'baimingdan' then 1 else null end) as is_testuser, 
max(case when cluster_name = 'da_rawmoney' then tag_value_num else null end) as da_rawmoney
from ta.user_result_cluster_9
group by 1
), 

log_info as(
select 
"$part_event" as event_name, 
"#event_time" as event_time, 
"#server_time" as server_time, 
"#user_id" as uid, "#account_id" as account_id, 
date("$part_date") as date, 
level as user_level, vip_level, 
date(da_install_ts) as da_install_date, 
date(da_firstpay_ts) as da_firstpay_date,
date(da_lastlogin_ts) as da_lastlogin_date,
date(da_lastpay_ts) as da_lastpay_date,
da_vip_level, da_user_level,
date_trunc('month', date("$part_date")) as first_of_month, 
last_day_of_month(date("$part_date")) as last_of_month, 
(case when "$part_event"='Payment' then payment_itemid 
when "$part_event"='PaymentDetail' then payment_item_id else null end) as payment_itemid, 
(case when "$part_event"='Payment' then rawmoney/100*0.052102 else null end) as rawmoney_rmb, 
(case when "$part_event"='PaymentDetail' then rawmoney*0.052102 else null end) as goods_rmb, 
items_detail, 
(case when "$part_event" in ('CostGold', 'AddGold') then itemid 
when "$part_event" = 'CostItem' then reason
when "$part_event" = 'AddItem' then item_add_reason
else null end) as reason, 
reason_id, reason_subid, 
item_add, item_cost, 
(case when "$part_event" in ('AddItem', 'CostItem') then itemid else null end) as item_id
from ta.v_event_9 a
left join new_user_tag_info b 
on a."#user_id" = b.uid 
left join ta_dim.dim_9_0_95498 c
on a.payment_itemid = c."payment_itemid@goodsid"
where "$part_date" >= '2024-03-01'
and "$part_date" <= '2024-05-31'
and is_testuser is null
and length("#account_id") > 6
and "$part_event" in ('Login', 'Payment', 'Logout', 'AddItem', 'CostItem', 'PaymentDetail')
), 

monthly_group as(
select first_of_month, 
uid, 
da_vip_level, da_user_level, 
da_install_date, da_lastlogin_date,
da_firstpay_date, da_lastpay_date,
sum(coalesce(rawmoney_rmb, 0)) as rawmoney_month, 
max(vip_level) as vip_level
from log_info
group by 1, 2, 3, 4, 5, 6, 7, 8
), 

monthly_cal as(
select first_of_month, 
date_add('month', -1, first_of_month) as last1_month, 
date_add('month', -2, first_of_month) as last2_month, 
date_add('month', 1, first_of_month) as next1_month, 
date_add('month', 2, first_of_month) as next2_month, 
uid, 
da_vip_level, da_user_level, 
da_install_date, da_lastlogin_date,
da_firstpay_date, da_lastpay_date, 
vip_level, 
coalesce(rawmoney_month, 0) as rawmoney_month, 
(case when vip_level = 0 then '[0]'
when vip_level <= 8 then '[1,8]'
when vip_level <= 11 then '[9,11]'
when vip_level <= 14 then '[12,14]'
when vip_level = 15 then '[15]'
else 'unknow' end) as vip_tag, 
(case when rawmoney_month >= 20000 then '超R'
when rawmoney_month >= 6000 then '大R'
when rawmoney_month >= 1000 then '中R'
when rawmoney_month >= 300 then '小R'
when rawmoney_month > 0 then '微R'
when rawmoney_month = 0 then '非R'
when rawmoney_month is null then '未活跃'
else 'unknow' end)  as current_tag, 
(case when rawmoney_month >= 20000 then 6
when rawmoney_month >= 6000 then 5
when rawmoney_month >= 1000 then 4
when rawmoney_month >= 300 then 3
when rawmoney_month > 0 then 2
when rawmoney_month = 0 then 1
when rawmoney_month is null then 0
else -1 end) as tag_index
from monthly_group
), 

lastmonth_tag as(
select distinct uid, tag_index, 
current_tag as constant_tag
from monthly_cal
where first_of_month = date('2024-04-01')
),  

currentmonth_tag as(
select distinct uid, tag_index, 
current_tag as currentconstant_tag
from monthly_cal
where first_of_month = date('2024-05-01')
), 

monthly_info as(
select 
coalesce(a.first_of_month, b.next1_month) as first_of_month, 
coalesce(a.last1_month, b.first_of_month) as last1_month, 
coalesce(a.last2_month, b.last1_month) as last2_month, 
coalesce(a.uid, b.uid) as uid, 
coalesce(a.vip_level, b.vip_level) as vip_level, 
coalesce(a.vip_tag, b.vip_tag) as vip_tag, 
coalesce(a.da_vip_level, b.da_vip_level) as da_vip_level, 
coalesce(a.da_user_level, b.da_user_level) as da_user_level, 
coalesce(a.da_install_date, b.da_install_date) as da_install_date, 
coalesce(a.da_lastlogin_date, b.da_lastlogin_date) as da_lastlogin_date, 
coalesce(a.da_firstpay_date, b.da_firstpay_date) as da_firstpay_date, 
coalesce(a.da_lastpay_date, b.da_lastpay_date) as da_lastpay_date, 
a.rawmoney_month, 
b.rawmoney_month as rawmoney_last1month, 
coalesce(a.current_tag, '未活跃') as current_tag, 
coalesce(b.current_tag, '未活跃') as last1_tag, 
-- coalesce(c.current_tag, '未活跃') as next1_tag, 
coalesce(a.tag_index, -1) as current_tagindex, 
coalesce(b.tag_index, -1) as last1_tagindex, 
-- coalesce(c.tag_index, -1) as next1_tagindex, 
coalesce(z.tag_index, -1) as constant_tagindex, 
coalesce(zz.tag_index, -1) as currentconstant_tagindex, 
coalesce(z.constant_tag, '未活跃') as constant_tag, 
coalesce(zz.currentconstant_tag, '未活跃') as currentconstant_tag, 
(case when a.rawmoney_month>0 then 1 else 0 end) as is_pay, 
1 as mau
from monthly_cal a
full join monthly_cal b
on a.last1_month = b.first_of_month and a.uid = b.uid
-- left join monthly_cal c
-- on a.next1_month = c.first_of_month and a.uid = c.uid
left join lastmonth_tag z
on a.uid = z.uid
left join currentmonth_tag zz
on a.uid = zz.uid
), 

monthly_tag as(
select first_of_month, last1_month, last2_month,
uid,
vip_level, vip_tag, da_vip_level,
da_user_level,
da_install_date, da_lastlogin_date,
da_firstpay_date, da_lastpay_date, 
rawmoney_month, rawmoney_last1month, 
current_tag, last1_tag, constant_tag, currentconstant_tag, 
current_tagindex, last1_tagindex, 
-- next1_tag, next1_tagindex, 
(case when current_tagindex>last1_tagindex then '升档'
when current_tagindex=last1_tagindex then '平档'
when current_tagindex<last1_tagindex then '降档'
else 'unknow' end) as status, 
-- (case when current_tagindex<next1_tagindex then '升档'
-- when current_tagindex=next1_tagindex then '平档'
-- when current_tagindex>next1_tagindex then '降档'
-- else 'unknow' end) as next_status, 
(case when constant_tagindex<currentconstant_tagindex then '升档'
when constant_tagindex=currentconstant_tagindex then '平档'
when constant_tagindex>currentconstant_tagindex then '降档'
else 'unknow' end) as constant_status, 
concat(constant_tag, '->', currentconstant_tag) as status_change, 
-- concat(current_tag, '->', next1_tag) as nextstatus_change, 
is_pay
from monthly_info
), 

paymentdetail_log as(
select event_name, event_time, server_time, 
date, first_of_month, uid, account_id, 
payment_itemid, goods_rmb, items_detail, 
row_number() over(partition by uid order by event_time, server_time, payment_itemid) as pay_rn
from log_info
where event_name = 'PaymentDetail'
), 

paymentdetail_explode_log as(
select event_name, event_time, server_time, 
date, first_of_month, uid, account_id, 
payment_itemid, goods_rmb, pay_rn, 
t.itemid as item_id, t.itemcount as item_count, 
'items' as goods_type
from paymentdetail_log, unnest(items_detail) as t
), 

paymentdetail_unexplode_log as(
select event_name, event_time, server_time, 
date, first_of_month, uid, account_id, 
payment_itemid, goods_rmb, pay_rn, 
null as item_id, null as item_count, 
'noitems' as goods_type
from paymentdetail_log
where items_detail is null 
), 

paymentdetail_unstack_log as(
select * from paymentdetail_explode_log
union all
select * from paymentdetail_unexplode_log
), 

paymentdetail_exchange_log as(
select event_name, event_time, server_time, 
date, first_of_month, uid, account_id, 
payment_itemid, goods_rmb, pay_rn, 
b."itemid@hero_rate" as hero_rate, 
b."itemid@is_special" as is_special, 
-- coalesce((case when b."itemid@is_special" = '1' then '武将' else b."itemid@system" end), '其他') as system, 
coalesce((case when b."itemid@system" = '活动' then '武将' 
when b."itemid@system" != '活动' and b."itemid@is_special" = '1' then '金币' 
else b."itemid@system" end), '其他') as system, 
coalesce(b."itemid@sub_system", '其他') as sub_system, 
goods_type, item_id, 
-- (case when b."itemid@is_special" = '1' then null else item_id end) as item_id_fit, 
a.item_count, 
b."itemid@item_name" as item_name, 
b."itemid@gold_value" as gold_value, 
a.item_count*b."itemid@gold_value" as paid_itemgoldnum
from paymentdetail_unstack_log a
left join ta_dim.dim_9_0_95031 b
on a.item_id = b."itemid@id"
), 

paymentdetail_cal_log as(
select event_name, event_time, server_time, 
date, first_of_month, uid, account_id, 
payment_itemid, goods_rmb, pay_rn, 
hero_rate, is_special, 
-- max(is_special) over(partition by uid, pay_rn order by event_time rows between unbounded preceding and unbounded following) as is_special, 
system, sub_system, 
concat(system, '-', sub_system) as system_group, 
goods_type, item_id, item_count, 
item_name, gold_value, paid_itemgoldnum, 
sum(paid_itemgoldnum) over(partition by uid, pay_rn order by event_time rows between unbounded preceding and unbounded following) as total_paid_itemgoldnum, 
(case when goods_type = 'noitems' then 1 
else paid_itemgoldnum/sum(paid_itemgoldnum) over(partition by uid, pay_rn order by event_time rows between unbounded preceding and unbounded following) end) as per_paid_itemgoldnum
from paymentdetail_exchange_log
), 

paymentdetail_agg_log as(
select event_name, event_time, server_time, 
date, first_of_month, uid, account_id, 
payment_itemid, goods_rmb, pay_rn, 
hero_rate, total_paid_itemgoldnum, 
is_special, system, sub_system, system_group, 
sum(per_paid_itemgoldnum) as per_paid_itemgoldnum
from paymentdetail_cal_log
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
), 

-- paymentdetail_array_log as(
-- select event_name, event_time, server_time, 
-- date, first_of_month, uid, account_id, 
-- payment_itemid, goods_rmb, pay_rn, 
-- hero_rate, total_paid_itemgoldnum, 
-- json_format(cast(map_agg(system_group, per_paid_itemgoldnum) as json)) as paymentdetail_sysper_detail
-- from paymentdetail_agg_log
-- group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
-- ), 

paymentdetail_sys_revenue_info as(
select event_name, event_time, server_time, 
date, first_of_month, uid, account_id, 
payment_itemid, goods_rmb, pay_rn, 
hero_rate, total_paid_itemgoldnum, 
is_special, system, sub_system, system_group, 
sum(case when system not in ('武将', '金币') then goods_rmb*per_paid_itemgoldnum else null end)as paymentdetail_sys_revenue, 
sum(goods_rmb*per_paid_itemgoldnum*hero_rate) as distributed_trans_hero_revenue, 
sum(case when system in ('武将', '金币') then goods_rmb*per_paid_itemgoldnum*(1-hero_rate) else null end)as paymentdetail_last_revenue
from paymentdetail_agg_log
-- where system not in ('武将', '金币')
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
), 

user_daily_sys_group_info as(
select date, first_of_month, uid, account_id, 
is_special, system, sub_system, system_group, 
sum(paymentdetail_sys_revenue) as paymentdetail_sys_revenue, 
sum(distributed_trans_hero_revenue) as distributed_trans_hero_revenue, 
sum(paymentdetail_last_revenue) as paymentdetail_last_revenue
from paymentdetail_sys_revenue_info
group by 1, 2, 3, 4, 5, 6, 7, 8
), 

user_daily_info as(
select date, first_of_month, uid, account_id, 
sum(paymentdetail_last_revenue) as paymentdetail_last_revenue
from paymentdetail_sys_revenue_info
group by 1, 2, 3, 4
), 

-- additem配置解析
item_log as(
select 
a.event_name, a.event_time, a.server_time, 
a.date, a.first_of_month, 
a.uid, a.account_id, 
a.item_id, 
b."itemid@item_name" as item_name, 
a.reason, a.reason_id, 
-- c."itemid@gold_name" as reason_cn, 
a.item_add, a.item_cost, 
b."itemid@gold_value" as gold_value, 
coalesce(b."itemid@system", '其他') as system, 
coalesce(b."itemid@sub_system", '其他') as sub_system, 
a.item_add * b."itemid@gold_value" as itemgoldnum
from log_info a
left join ta_dim.dim_9_0_95031 b
on a.item_id = b."itemid@id"
where event_name = 'AddItem'
and reason_id != 1
and b."itemid@system" != '金币'
and b."itemid@is_special" is null
and b."itemid@gold_value" > 0
), 

user_daily_item_info as(
select date, first_of_month, uid, account_id, 
sum(itemgoldnum) as item_gold_num
from item_log
where date>=date('2023-12-16')
group by 1, 2, 3, 4
), 

user_daily_goldvalue_info as(
select a.date, a.first_of_month, a.uid, a.account_id, 
a.paymentdetail_last_revenue, 
b.item_gold_num, 
a.paymentdetail_last_revenue/item_gold_num as gold_value
from user_daily_info a
left join user_daily_item_info b
on a.uid = b.uid and a.date = b.date
), 

user_daily_item_sys_group as(
select date, first_of_month, uid, account_id, 
system, sub_system, 
concat(system, '-', sub_system) as system_group, 
sum(itemgoldnum) as item_gold_num
from item_log
group by 1, 2, 3, 4, 5, 6
), 

user_daily_item_sys_group_info as(
select a.date, a.first_of_month, a.uid, a.account_id, 
a.system, a.sub_system, a.system_group, 
b.gold_value, 
sum(a.item_gold_num) as item_gold_num, 
sum(a.item_gold_num*b.gold_value) as item_sys_revenue
from user_daily_item_sys_group a
left join user_daily_goldvalue_info b
on a.uid = b.uid and a.date = b.date
group by 1, 2, 3, 4, 5, 6, 7, 8
), 

sys_revenue_res as(
select
coalesce(a.date, b.date) as date, 
date_trunc('week', coalesce(a.date, b.date)) as natural_week, 
coalesce(a.first_of_month, b.first_of_month) as first_of_month, 
-- coalesce(a.uid, b.uid) as uid, 
-- coalesce(a.account_id, b.account_id) as account_id, 
coalesce(a.system, b.system) as system, 
coalesce(a.sub_system, b.sub_system) as sub_system, 
coalesce(a.system_group, b.system_group) as system_group, 
c.current_tag, c.last1_tag, 
-- c.next1_tag, 
c.constant_tag, c.currentconstant_tag, 
-- c.last1_tagindex, c.next1_tagindex, c.current_tagindex, 
c.status, c.status_change, 
-- a.gold_value, 
sum(a.item_gold_num) as item_gold_num, 
sum(b.paymentdetail_sys_revenue) as paymentdetail_sys_revenue,
sum(b.distributed_trans_hero_revenue) as distributed_trans_hero_revenue,
sum(a.item_sys_revenue) as item_sys_revenue, 
sum(coalesce(b.paymentdetail_sys_revenue, 0) + coalesce(b.distributed_trans_hero_revenue, 0) + coalesce(a.item_sys_revenue, 0)) as revenue
from user_daily_item_sys_group_info a
full join user_daily_sys_group_info b
on a.uid = b.uid
and a.date = b.date
and a.system = b.system
and a.sub_system = b.sub_system
left join monthly_tag c
on coalesce(a.uid, b.uid) = c.uid
and coalesce(a.first_of_month, b.first_of_month) = c.first_of_month 
where coalesce(a.date, b.date)>=date('2024-04-01')
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)

-- select *
-- from sys_revenue_res 
-- where revenue>0

select *
from sys_revenue_res 
where revenue>0

