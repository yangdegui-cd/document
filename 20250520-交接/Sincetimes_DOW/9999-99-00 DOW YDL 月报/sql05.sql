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
payment_itemid, 
rawmoney/100 as rawmoney, 
rawmoney/100*0.052102 as rawmoney_rmb, 
(case when "$part_event" in ('CostGold', 'AddGold') then itemid 
when "$part_event" = 'CostItem' then reason
when "$part_event" = 'AddItem' then item_add_reason
else null end) as reason, 
item_add, item_cost, 
(case when "$part_event" in ('AddItem', 'CostItem') then itemid else null end) as item_id
from ta.v_event_9 a
left join new_user_tag_info b 
on a."#user_id" = b.uid 
left join ta_dim.dim_9_0_95498 c
on a.payment_itemid = c."payment_itemid@goodsid"
where "$part_date" >= '2023-06-01'
and "$part_date" <= '2025-04-30'
and is_testuser is null
and length("#account_id") > 6
and "$part_event" in ('Login', 'Logout', 'Register', 'Payment')
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
where first_of_month = date('2025-03-01')
), 

currentmonth_tag as(
select distinct uid, tag_index, 
current_tag as currentconstant_tag
from monthly_cal
where first_of_month = date('2025-04-01')
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
coalesce(z.tag_index, -1) as constant_tagindex, 
coalesce(zz.tag_index, -1) as currentconstant_tagindex, 
coalesce(z.constant_tag, '未活跃') as constant_tag, 
coalesce(zz.currentconstant_tag, '未活跃') as currentconstant_tag, 
(case when a.rawmoney_month>0 then 1 else 0 end) as is_pay, 
1 as mau
from monthly_cal a
full join monthly_cal b
on a.last1_month = b.first_of_month and a.uid = b.uid
left join monthly_cal c
on a.next1_month = c.first_of_month and a.uid = c.uid
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
current_tag, last1_tag, constant_tag, currentconstant_tag, next1_tag, 
current_tagindex, last1_tagindex, next1_tagindex, 
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

payment_info as(
select first_of_month, 
date_add('month', -1, first_of_month) as last1_month, 
date_add('month', -2, first_of_month) as last2_month, 
uid, 
b."payment_itemid@pay_typename" as goods_cate, 
b."payment_itemid@item_name" as goods_name, 
a.rawmoney_rmb, 
row_number() over(partition by a.uid, a.first_of_month order by event_time, server_time) as rn
from log_info a
left join ta_dim.dim_9_0_95498 b
on a.payment_itemid = b."payment_itemid@goodsid"
where event_name = 'Payment'
)

-- mau_info as(
-- select first_of_month, last1_tag, 
-- count(distinct uid) as mau
-- from monthly_tag
-- where last1_tag != '未活跃'
-- group by 1, 2
-- ), 
select
a.first_of_month, 
a.last1_month, 
a.uid, 
z.vip_level, z.vip_tag,
z.current_tag, z.last1_tag, z.next1_tag, z.constant_tag, z.currentconstant_tag, 
-- z.last1_tagindex, z.next1_tagindex, z.current_tagindex, 
z.status, z.status_change, z.constant_status, 
-- z.next_status, z.nextstatus_change, 
a.goods_cate, a.goods_name, 
a.rawmoney_rmb, a.rn
from payment_info a
left join monthly_tag z
on a.uid = z.uid and a.first_of_month = z.first_of_month
