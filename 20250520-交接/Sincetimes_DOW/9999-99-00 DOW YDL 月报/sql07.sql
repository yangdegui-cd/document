with month_tag as(
select month, role_id, pay_tag, is_test
from hive.dow_jpnew_w.dws_user_info_mi
where month = date('2025-03-01')
), 

paymentdetail_log as(
select part_date, date(part_date) as date, date_trunc('month', date(part_date)) as first_of_month, event_time, a.role_id, 
payment_item_id as payment_itemid, items_detail, 
a.rawmoney * 0.052102 as money_rmb, sincetime_money * c.price * 0.052102 as sincetime_money, sincetime_money as token_cost, 
-- c.token_end, c.token_end * c.price * 1.06163783 as pearl_revenue, 
row_number() over(partition by a.role_id order by event_time, payment_item_id) as pay_rn
from hive.dow_jpnew_r.dwd_gserver_paymentdetail_live a
left join hive.dow_jpnew_w.dws_token_info_mf c
on a.role_id = c.role_id and date_trunc('month', date(part_date)) = c.cal_month
left join hive.dow_jpnew_w.dim_gserver_base_roleid d
on a.role_id = d.role_id
where part_date >= '2025-03-01'
and part_date <= '2025-04-30'
and payment_item_id not in ('gold_100001', 'gold_100002', 'gold_100003', 'gold_100004', 'gold_100005', 'gold_100006')
and d.role_id is null
), 

paymentdetail_explode_log as(
select part_date, date, event_time, role_id, 
payment_itemid, items_detail,
money_rmb, sincetime_money, pay_rn, 
json_extract_scalar(t.item, '$.item_id') as item_id,
cast(json_extract_scalar(t.item, '$.itemcount') as bigint) as item_count, 
'items' as goods_type
from paymentdetail_log
cross join unnest(items_detail) as t(item)
), 

paymentdetail_unexplode_log as(
select part_date, date, event_time, role_id, 
payment_itemid, items_detail,
money_rmb, sincetime_money, pay_rn, 
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
select part_date, date, event_time, role_id, 
payment_itemid, items_detail,
money_rmb, sincetime_money, pay_rn, 
coalesce(b.hero_rate, 0) as hero_rate, b.is_special, 
coalesce((case when b.system = '活动' then '虚拟武将' 
when b.system != '活动' and b.is_special = '1' then '金币' 
else b.system end), '其他') as system, 
coalesce(b.sub_system, '其他') as sub_system, 
goods_type, a.item_id, 
(case when cast(a.item_id as bigint) - 11000 <= 0 then null
when b.sub_system = '召唤卡' then cast(a.item_id as bigint) - 11000
when b.sub_system = '武将碎片' then cast(a.item_id as bigint) - 12000
when cast(a.item_id as bigint) >= 1000 and cast(a.item_id as bigint) <= 1999 then cast(a.item_id as bigint)
else null end) as hero_id, 
a.item_count, 
b.item_name, gold_value, 
a.item_count * b.gold_value as paid_itemgoldnum
from paymentdetail_unstack_log a
left join hive.dow_jpnew_w.dim_gserver_additem_itemid b
on a.item_id = b.item_id
), 

paymentdetail_cal_log as(
select part_date, date, event_time, role_id, 
payment_itemid, items_detail,
money_rmb, sincetime_money, pay_rn, 
hero_rate, is_special, 
system, sub_system, 
concat(system, '-', sub_system) as system_group, 
goods_type, item_id, item_count, 
a.hero_id, 
(case when system in ('武将', '虚拟武将') then coalesce(b.hero_cn, sub_system)
else null end) as hero_name, 
item_name, gold_value, paid_itemgoldnum, 
sum(paid_itemgoldnum) over(partition by role_id, pay_rn order by event_time rows between unbounded preceding and unbounded following) as total_paid_itemgoldnum, 
(case when goods_type = 'noitems' then 1 
else paid_itemgoldnum/sum(paid_itemgoldnum) over(partition by role_id, pay_rn order by event_time rows between unbounded preceding and unbounded following) end) as per_paid_itemgoldnum
from paymentdetail_exchange_log a
left join hive.dow_jpnew_w.dim_gserver_levelup_heroid b
on cast(a.hero_id as varchar) = b.hero_id
), 

paymentdetail_agg_log as(
select part_date, date, role_id, 
payment_itemid, items_detail,
money_rmb, sincetime_money, pay_rn, 
hero_rate, is_special, 
system, sub_system, system_group, 
hero_id, hero_name, 
sum(per_paid_itemgoldnum) as per_paid_itemgoldnum
from paymentdetail_cal_log
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
), 

paymentdetail_sys_revenue_info as(
select part_date, date, date_trunc('week', date) as week, 
date_trunc('month', date) as first_of_month, role_id, 
payment_itemid, items_detail,
money_rmb, sincetime_money, pay_rn, 
hero_rate, is_special, 
system, sub_system, system_group, 
hero_id, hero_name, 
sum(case when system not in ('虚拟武将', '金币') then money_rmb*per_paid_itemgoldnum else null end)as paymentdetail_sys_revenue, 
sum(case when system in ('虚拟武将', '金币') then money_rmb*per_paid_itemgoldnum*hero_rate else null end) as distributed_trans_hero_revenue, 
sum(case when system in ('虚拟武将', '金币') then money_rmb*per_paid_itemgoldnum*(1-hero_rate) else null end)as paymentdetail_last_revenue, 
sum(case when system not in ('虚拟武将', '金币') then sincetime_money*per_paid_itemgoldnum else null end)as paymentdetail_sys_pearl, 
sum(case when system in ('虚拟武将', '金币') then sincetime_money*per_paid_itemgoldnum*hero_rate else null end) as distributed_trans_hero_pearl, 
sum(case when system in ('虚拟武将', '金币') then sincetime_money*per_paid_itemgoldnum*(1-hero_rate) else null end)as paymentdetail_last_pearl
from paymentdetail_agg_log
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
), 

user_daily_sys_group_info as(
select date, week, first_of_month, a.role_id, 
c.pay_tag, is_special, system, sub_system, system_group, 
hero_id, hero_name, 
sum(paymentdetail_sys_revenue) as paymentdetail_sys_revenue, 
sum(distributed_trans_hero_revenue) as distributed_trans_hero_revenue, 
sum(paymentdetail_last_revenue) as paymentdetail_last_revenue, 
sum(paymentdetail_sys_pearl) as paymentdetail_sys_pearl, 
sum(distributed_trans_hero_pearl) as distributed_trans_hero_pearl, 
sum(paymentdetail_last_pearl) as paymentdetail_last_pearl
from paymentdetail_sys_revenue_info a
left join month_tag c
on a.role_id = c.role_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
), 

user_daily_info as(
select date, week, first_of_month, role_id, 
sum(coalesce(paymentdetail_last_revenue, 0) + coalesce(paymentdetail_last_pearl, 0)) as paymentdetail_last_revenue
from paymentdetail_sys_revenue_info
group by 1, 2, 3, 4
), 

-- additem配置解析
item_log as(
select part_date, date(part_date) as date, 
date_trunc('week', date(part_date)) as week, 
date_trunc('month', date(part_date)) as first_of_month, 
event_time, 
a.role_id, 
a.item_id, b.item_name, 
a.reason, a.reason_id, 
a.item_num as item_add, 
b.gold_value as gold_value, 
coalesce(b.system, '其他') as system, 
coalesce(b.system, '其他') as sub_system, 
(case when cast(a.item_id as bigint) - 11000 <= 0 then null
when b.sub_system = '召唤卡' then cast(a.item_id as bigint) - 11000
when b.sub_system = '武将碎片' then cast(a.item_id as bigint) - 12000
when cast(a.item_id as bigint) >= 1000 and cast(a.item_id as bigint) <= 1999 then cast(a.item_id as bigint)
else null end) as hero_id, 
a.item_num * b.gold_value as itemgoldnum
from hive.dow_jpnew_r.dwd_gserver_itemchange_live a
left join hive.dow_jpnew_w.dim_gserver_additem_itemid b
on a.item_id = b.item_id
where event_type = 'gain'
and reason_id != '1'
and b.system not in ('金币', '珍珠')
and b.is_special != '1'
and b.gold_value > 0
and part_date >= '2025-03-01'
and part_date <= '2025-04-30'
), 

user_daily_item_info as(
select date, week, first_of_month, role_id, 
sum(itemgoldnum) as item_gold_num
from item_log
where date>=date('2023-12-16')
group by 1, 2, 3, 4
), 

user_daily_goldvalue_info as(
select a.date, a.week, a.first_of_month, a.role_id, 
b.item_gold_num, 
a.paymentdetail_last_revenue/item_gold_num as gold_value
from user_daily_info a
left join user_daily_item_info b
on a.role_id = b.role_id and a.date = b.date
), 

user_daily_item_sys_group as(
select date, week, first_of_month, role_id, 
system, sub_system, a.hero_id, 
(case when system in ('武将', '虚拟武将') then coalesce(b.hero_cn, sub_system)
else null end) as hero_name, 
concat(system, '-', sub_system) as system_group, 
sum(itemgoldnum) as item_gold_num
from item_log a
left join hive.dow_jpnew_w.dim_gserver_levelup_heroid b
on cast(a.hero_id as varchar) = b.hero_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
), 

user_daily_item_sys_group_info as(
select a.date, a.week, a.first_of_month, a.role_id, 
c.pay_tag, a.system, a.sub_system, a.system_group, 
a.hero_id, a.hero_name, 
b.gold_value, 
sum(a.item_gold_num) as item_gold_num, 
sum(a.item_gold_num*b.gold_value) as item_sys_revenue
from user_daily_item_sys_group a
left join user_daily_goldvalue_info b
on a.role_id = b.role_id and a.date = b.date
left join month_tag c
on a.role_id = c.role_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
), 

sys_revenue_res as(
select 
-- coalesce(a.date, b.date) as date, 
-- coalesce(a.week, b.week) as week, 
coalesce(a.first_of_month, b.first_of_month) as first_of_month, 
-- coalesce(a.role_id, b.role_id) as role_id, 
coalesce(a.pay_tag, b.pay_tag) as pay_tag, 
-- (case when coalesce(a.hero_id, b.hero_id) in (1304, 1305) then '新武将' 
-- when coalesce(a.system, b.system) = '虚拟武将' then '武将' else coalesce(a.system, b.system) end) as system, 
(case when coalesce(a.system, b.system) = '虚拟武将' then '武将' else coalesce(a.system, b.system) end) as system, 
coalesce(a.sub_system, b.sub_system) as sub_system, 
-- coalesce(a.system_group, b.system_group) as system_group, 
coalesce(a.hero_id, b.hero_id) as hero_id, 
coalesce(a.hero_name, b.hero_name) as hero_name, 
sum(a.item_gold_num) as item_gold_num, 
-- sum(b.paymentdetail_sys_revenue) as paymentdetail_sys_revenue,
-- sum(b.distributed_trans_hero_revenue) as distributed_trans_hero_revenue,
-- sum(a.item_sys_revenue) as item_sys_revenue, 
sum(coalesce(b.paymentdetail_sys_revenue, 0) + coalesce(b.distributed_trans_hero_revenue, 0) + coalesce(a.item_sys_revenue, 0)) as revenue, 
-- sum(b.paymentdetail_sys_pearl) as paymentdetail_sys_pearl, 
-- sum(b.distributed_trans_hero_pearl) as distributed_trans_hero_pearl, 
sum(b.paymentdetail_last_pearl) as paymentdetail_last_pearl, 
sum(coalesce(b.paymentdetail_sys_pearl, 0) + coalesce(b.distributed_trans_hero_pearl, 0)) as pearl_revenue
from user_daily_item_sys_group_info a
full join user_daily_sys_group_info b
on a.role_id = b.role_id
and a.date = b.date
and a.system = b.system
and a.sub_system = b.sub_system
and a.hero_id = b.hero_id
-- left join month_paytag c
-- on coalesce(a.role_id, b.role_id) = c.role_id
-- and coalesce(a.first_of_month, b.first_of_month) = c.month_agg 
-- where c.role_id is not null
group by 1, 2, 3, 4, 5, 6
), 

sys_revenue_join as(
select first_of_month, pay_tag, 
system, sub_system, hero_id, hero_name, 
revenue, pearl_revenue, paymentdetail_last_pearl, 
revenue + pearl_revenue as total_revenue
from sys_revenue_res
), 

paymentdetail_agg as(
select first_of_month, role_id, sum(token_cost) as token_cost
from paymentdetail_log
group by 1, 2
), 

pearl_money as(
select cal_month as first_of_month, pay_tag, 
sum(coalesce(a.token_cost*price * 0.052102, 0) - coalesce(c.token_cost*price * 0.052102, 0)) as token_diff, 
sum(token_end*price * 0.052102) as pearl_revenue
from hive.dow_jpnew_w.dws_token_info_mf a
left join month_tag b
on a.role_id = b.role_id
left join paymentdetail_agg c
on a.role_id = c.role_id and a.cal_month = c.first_of_month
where cal_month >= date '2024-01-01'
and cal_month <= date '2025-04-30'
group by 1, 2
), 

pearl_money_col01 as(
select first_of_month, pay_tag, '珍珠' as system, '珍珠' as sub_system, 
null as hero_id, null as hero_name, 
null as revenue, pearl_revenue, null as paymentdetail_last_pearl, pearl_revenue as total_revenue
from pearl_money
), 

pearl_money_col02 as(
select first_of_month, pay_tag, '珍珠' as system, '珍珠其他' as sub_system, 
null as hero_id, null as hero_name, 
null as revenue, token_diff as pearl_revenue, null as paymentdetail_last_pearl, token_diff as total_revenue
from pearl_money
)

select * from pearl_money_col01
union all
select * from pearl_money_col02
union all
select * from sys_revenue_join