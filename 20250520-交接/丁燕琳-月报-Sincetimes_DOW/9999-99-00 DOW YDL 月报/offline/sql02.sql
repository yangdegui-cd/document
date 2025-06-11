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
-- date_trunc('week', date("$part_date")) as start_natualweek, 
-- 7-day_of_week(date("$part_date")) as sunday_diff, 
-- day_of_week(date("$part_date"))-1 as monday_diff, 
(case when "$part_event"='Payment' then payment_itemid 
when "$part_event"='PaymentDetail' then payment_item_id else null end) as payment_itemid, 
(case when "$part_event"='Payment' then rawmoney/100*0.052102 
when "$part_event"='PaymentDetail' then rawmoney*0.052102 else null end) as rawmoney_rmb, 
sincetime_money, 
(case when "$part_event" in ('CostGold', 'AddGold') then itemid 
when "$part_event" = 'CostItem' then reason
when "$part_event" = 'AddItem' then item_add_reason
else null end) as reason, 
reason_id, reason_subid, 
item_add, item_cost, 
(case when "$part_event" in ('AddItem', 'CostItem') then itemid else null end) as item_id, 
items_detail
from ta.v_event_9 a
left join new_user_tag_info b 
on a."#user_id" = b.uid 
left join ta_dim.dim_9_0_95498 c
on a.payment_itemid = c."payment_itemid@goodsid"
where "$part_date" >= '2023-11-01'
and "$part_date" <= '2024-01-31'
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
(case when rawmoney_month >= 6000 then '超R'
when rawmoney_month >= 2000 then '大R'
when rawmoney_month >= 1000 then '中R'
when rawmoney_month >= 200 then '小R'
when rawmoney_month > 0 then '微R'
when rawmoney_month = 0 then '非R'
when rawmoney_month is null then '未活跃'
else 'unknow' end)  as current_tag, 
(case when rawmoney_month >= 6000 then 6
when rawmoney_month >= 2000 then 5
when rawmoney_month >= 1000 then 4
when rawmoney_month >= 200 then 3
when rawmoney_month > 0 then 2
when rawmoney_month = 0 then 1
when rawmoney_month is null then 0
else -1 end)  as tag_index
from monthly_group
), 

lastmonth_tag as(
select distinct uid, current_tag as constant_tag
from monthly_cal
where first_of_month = date('2023-12-01')
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
coalesce(c.current_tag, '未活跃') as next1_tag, 
coalesce(a.tag_index, -1) as current_tagindex, 
coalesce(b.tag_index, -1) as last1_tagindex, 
coalesce(c.tag_index, -1) as next1_tagindex, 
coalesce(z.constant_tag, '未活跃') as constant_tag, 
(case when a.rawmoney_month>0 then 1 else 0 end) as is_pay, 
1 as mau
from monthly_cal a
full join monthly_cal b
on a.last1_month = b.first_of_month and a.uid = b.uid
left join monthly_cal c
on a.next1_month = c.first_of_month and a.uid = c.uid
left join lastmonth_tag z
on a.uid = z.uid
), 

monthly_tag as(
select first_of_month, last1_month, last2_month,
uid,
vip_level, vip_tag, da_vip_level,
da_user_level,
da_install_date, da_lastlogin_date,
da_firstpay_date, da_lastpay_date, 
rawmoney_month, rawmoney_last1month, 
current_tag, last1_tag, constant_tag, next1_tag, 
current_tagindex, last1_tagindex, next1_tagindex, 
(case when current_tagindex>last1_tagindex then '升档'
when current_tagindex=last1_tagindex then '平档'
when current_tagindex<last1_tagindex then '降档'
else 'unknow' end) as status, 
(case when current_tagindex<next1_tagindex then '升档'
when current_tagindex=next1_tagindex then '平档'
when current_tagindex>next1_tagindex then '降档'
else 'unknow' end) as next_status, 
concat(constant_tag, '->', current_tag) as status_change, 
concat(current_tag, '->', next1_tag) as nextstatus_change, 
is_pay
from monthly_info
), 

-- paymentdetail配置解析
paymentdetail_log as(
select
event_name, event_time, server_time, 
date, first_of_month, 
uid, account_id, 
rawmoney_rmb, sincetime_money, 
items_detail, 
t.itemid as item_id, t.itemcount as item_count
from log_info, unnest(items_detail) as t
where event_name = 'PaymentDetail'
), 

paymentdetail_exchange_log as(
select
event_name, event_time, server_time, 
date, first_of_month, 
uid, account_id, 
item_id, 
b."itemid@item_name" as item_name, 
b."itemid@gold_value" as gold_value, 
coalesce(b."itemid@system", '其他') as system, 
coalesce(b."itemid@sub_system", '其他') as sub_system, 
a.item_count, 
a.item_count * b."itemid@gold_value" as itemgoldnum, 
a.rawmoney_rmb
from paymentdetail_log a
left join ta_dim.dim_9_0_95031 b
on a.item_id = b."itemid@id"
), 

-- 礼包金币数
daily_paymentdetail_group as(
select date, uid, account_id, first_of_month, 
-- 礼包金币个数
sum(case when item_id = 1 then item_count else 0 end) as paid_goldnum, 
sum(case when item_id != 1 then itemgoldnum else 0 end) as paid_itemgoldnum, 
sum(case when item_id is null then rawmoney_rmb else 0 end) as other_revenue

from paymentdetail_exchange_log
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
-- left join ta_dim.dim_9_0_95031 c
-- on a.reason = c."itemid@id"
where event_name = 'AddItem'
and b."itemid@is_special" is null
and b."itemid@gold_value" > 0
and reason_id != 1
and item_id != 1
), 

-- 非礼包内容
daily_item_group as(
select date, uid, first_of_month, 
sum(itemgoldnum) as exchange_itemgoldnum
from item_log
group by 1, 2, 3
), 

-- 日数据
daily_group as(
select date, uid, account_id, first_of_month, 
sum(rawmoney_rmb) as rawmoney_rmb
from log_info
where event_name = 'Payment'
group by 1, 2, 3, 4
), 

-- 选择日金币聚合维度
user_goldvalue_daily_info as(
select a.date, a.uid, a.account_id, 
a.first_of_month, 
a.rawmoney_rmb, 
b.paid_goldnum, b.paid_itemgoldnum, 
c.exchange_itemgoldnum, 
-- -- 礼包金币价格
a.rawmoney_rmb/(b.paid_goldnum+b.paid_itemgoldnum) as paid_goldrmb, 

-- -- 兑换金币价格
(a.rawmoney_rmb/(b.paid_goldnum+b.paid_itemgoldnum) * b.paid_goldnum)/c.exchange_itemgoldnum as exchange_goldrmb, 
-- -- 处理没有花费的用户
(case when b.paid_goldnum>0 and c.exchange_itemgoldnum is null then a.rawmoney_rmb/(b.paid_goldnum+b.paid_itemgoldnum) * b.paid_goldnum 
    else null end) as other_revenue
from daily_group a
left join daily_paymentdetail_group b
on a.uid = b.uid and a.date = b.date
left join daily_item_group c
on a.uid = c.uid and a.date = c.date
), 

-- paymentdetail 收入
paymentdetail_revenue as(
select a.date, a.first_of_month, 
a.uid, a.account_id, 
a.system, a.sub_system, 
sum(a.itemgoldnum*b.paid_goldrmb) as paid_item_revenue
from paymentdetail_exchange_log a
left join user_goldvalue_daily_info b
on a.uid = b.uid and a.date = b.date
where item_id != 1
group by 1, 2, 3, 4, 5, 6
), 

-- additem 收入
additem_revenue as(
select a.date, a.first_of_month, 
a.uid, a.account_id, 
a.system, a.sub_system, 
sum(a.itemgoldnum*b.exchange_goldrmb) as exchange_item_revenue
from item_log a
left join user_goldvalue_daily_info b
on a.uid = b.uid and a.date = b.date
group by 1, 2, 3, 4, 5, 6
), 

-- paymentdetail&additem
revenue_info as(
select
coalesce(a.date, b.date) as date,  
coalesce(a.first_of_month, b.first_of_month) as first_of_month,  
coalesce(a.uid, b.uid) as uid,  
coalesce(a.account_id, b.account_id) as account_id,  
coalesce(a.system, b.system) as system,  
coalesce(a.sub_system, b.sub_system) as sub_system,  
a.paid_item_revenue, 
b.exchange_item_revenue
from paymentdetail_revenue a
full join additem_revenue b
on a.uid = b.uid and a.date = b.date
and a.system = b.system and a.sub_system = b.sub_system
), 

-- 权益类礼包
paymentdetail_other_revenue as(
select date, 
uid, account_id, first_of_month, 
null as paid_goldrmb, 
null as exchange_goldrmb, 
'其他' as system, '其他' as sub_system, 
null as paid_item_revenue, 
other_revenue as exchange_item_revenue, 
other_revenue as revenue
from daily_paymentdetail_group
), 

-- 道具当日没兑换花费
additem_other_revenue as(
select date, 
uid, account_id, first_of_month, 
-- null as paid_goldnum, 
-- null as paid_itemgoldnum, 
-- null as exchange_itemgoldnum, 
null as paid_goldrmb, 
null as exchange_goldrmb, 
'其他' as system, '其他' as sub_system, 
null as paid_item_revenue, 
other_revenue as exchange_item_revenue, 
other_revenue as revenue
from user_goldvalue_daily_info
), 

system_revenue as(
select 
a.date, a.uid, a.account_id, a.first_of_month, 
-- z.current_tag, z.last1_tag, z.next1_tag, z.constant_tag, 
-- z.status, z.status_change, 
-- z.next_status, z.nextstatus_change, 
-- a.paid_goldnum, a.paid_itemgoldnum, a.exchange_itemgoldnum, 
a.paid_goldrmb, a.exchange_goldrmb, 
b.system, b.sub_system, 
b.paid_item_revenue, b.exchange_item_revenue, 
(coalesce(b.paid_item_revenue, 0)+coalesce(b.exchange_item_revenue, 0)) as revenue
from user_goldvalue_daily_info a
left join revenue_info b
on a.uid = b.uid and a.date = b.date
-- 
), 

union_res as(
select * from paymentdetail_other_revenue
union all
select * from additem_other_revenue
union all
select * from system_revenue
)

-- select date, a.uid, a.account_id, a.first_of_month, 
-- z.current_tag, z.last1_tag, z.next1_tag, z.constant_tag, 
-- z.status, z.status_change, 
-- z.next_status, z.nextstatus_change, 
-- -- a.paid_goldnum, a.paid_itemgoldnum, a.exchange_itemgoldnum, 
-- a.paid_goldrmb, a.exchange_goldrmb, 
-- a.system, a.sub_system, 
-- a.paid_item_revenue, a.exchange_item_revenue, a.revenue
-- from union_res a
-- left join monthly_tag z
-- on a.uid = z.uid and a.first_of_month = z.first_of_month
-- where a.date >= date('2023-12-14')

-- select *
-- from paymentdetail_other_revenue
-- where account_id = '13365938420551'