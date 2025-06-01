# check1
with item_unnest as
(select date, role_id, itemadd_daily_array, items.item_info,
json_extract_scalar(items.item_info, '$.item_id') as item_id,
json_extract_scalar(items.item_info, '$.item_name') as item_name,
json_extract_scalar(items.item_info, '$.item_type') as item_type,
json_extract_scalar(items.item_info, '$.item_num') as item_num
from hive.dow_jp_w.dws_audit_daily_di
cross join unnest(itemadd_daily_array) as items(item_info)
)

select item_id, item_name, item_type, count(item_num) as item_num, count(distinct role_id) as users, min(date) as min_date, max(date) as max_date
from item_unnest
where item_name is null
group by 1, 2, 3
order by 1
;

# check2
select date, role_id, core_cost, core_cost_addmoney, corecost_play,
reduce(itemadd_daily_array, 0, (s, x) -> 
s + coalesce((case when json_extract_scalar(x, '$.item_type') = 'once' then json_value(x, 'strict $.core_cost' returning double) else null end), 0), 
s -> s) as corecost_once,
reduce(itemadd_daily_array, 0, (s, x) -> 
s + coalesce((case when json_extract_scalar(x, '$.item_type') = 'forever' then json_value(x, 'strict $.core_cost' returning double) else null end), 0), 
s -> s) as corecost_forever,
reduce(itemadd_daily_array, 0, (s, x) -> 
s + coalesce((case when json_extract_scalar(x, '$.item_type') is null or json_extract_scalar(x, '$.item_type') not in ('once', 'forever') then 1 else null end), 0), 
s -> s) as corecost_other,
coalesce(corecost_play, 0) + reduce(itemadd_daily_array, 0, (s, x) -> 
s + coalesce(json_value(x, 'strict $.core_cost' returning double), 0), 
s -> s) - core_cost_addmoney  as corecost_cal,
corecost_play_array, itemadd_daily_array
from hive.dow_jp_w.dws_audit_daily_di
where role_id = '2535478157193130'
--limit 100
;


# month agg
with user_daily as
(select date, date_trunc('month', date) as month,
role_id, channel,
money, core_add_addmoney as core_add, core_cost_addmoney as core_cost,
reduce(itemadd_daily_array, 0, (s, x) -> 
s + coalesce((case when json_extract_scalar(x, '$.item_type') = 'once' then json_value(x, 'strict $.core_cost' returning double) else null end), 0), 
s -> s) as corecost_once,
reduce(itemadd_daily_array, 0, (s, x) -> 
s + coalesce((case when json_extract_scalar(x, '$.item_type') = 'forever' then json_value(x, 'strict $.core_cost' returning double) else null end), 0), 
s -> s) as corecost_forever,
corecost_play,
itemadd_daily_array
from hive.dow_jp_w.dws_audit_daily_di
where part_date >= '2024-01-01'
)

select month, channel, 
sum(money) as money,
sum(core_add) as core_add,
sum(core_cost) as core_cost,
sum(corecost_once) as corecost_once,
sum(corecost_forever) as corecost_forever,
sum(corecost_play) as corecost_play
from user_daily
group by 1, 2
;

# once item
with user_daily as
(select date, date_trunc('month', date) as month,
role_id, channel,
filter(itemadd_daily_array, x -> json_extract_scalar(x, '$.item_type') = 'once') as itemadd_once,
filter(itemcost_daily_array, x -> json_extract_scalar(x, '$.item_type') = 'once') as itemcost_once
from hive.dow_jp_w.dws_audit_daily_di
where part_date >= '2024-01-01'
),

user_itemadd_daily_unnest as
(select date, month,
role_id, channel,
itemadd_once, itemcost_once,
json_extract_scalar(items.item_info, '$.item_id') as item_id,
json_extract_scalar(items.item_info, '$.item_name') as item_name,
json_value(items.item_info, 'strict $.item_num' returning bigint) as item_num,
json_value(items.item_info, 'strict $.core_cost' returning double) as core_cost
from user_daily a
cross join unnest(itemadd_once) as items(item_info)
),

user_itemadd_daily as
(select month, channel, item_id, item_name, sum(item_num) as item_add, sum(core_cost) as core_cost
from user_itemadd_daily_unnest
group by 1, 2, 3, 4
),

user_itemcost_daily_unnest as
(select date, month,
role_id, channel,
itemcost_once, itemcost_once,
json_extract_scalar(items.item_info, '$.item_id') as item_id,
json_value(items.item_info, 'strict $.item_num' returning bigint) as item_num
from user_daily a
cross join unnest(itemcost_once) as items(item_info)
),

user_itemcost_daily as
(select month, channel, item_id, sum(item_num) as item_cost
from user_itemcost_daily_unnest
group by 1, 2, 3
)

select a.*, b.item_cost
from user_itemadd_daily a
left join user_itemcost_daily b
on a.month = b.month and a.channel = b.channel
and a.item_id = b.item_id
order by 1
;


# forever
with user_daily as
(select date, date_trunc('month', date) as month,
role_id, channel,
filter(itemadd_daily_array, x -> json_extract_scalar(x, '$.item_type') = 'forever') as itemadd_forever
from hive.dow_jp_w.dws_audit_daily_di
where part_date >= '2024-01-01'
),

user_itemadd_daily_unnest as
(select date, month,
role_id, channel,
itemadd_forever,
json_extract_scalar(items.item_info, '$.item_id') as item_id,
json_extract_scalar(items.item_info, '$.item_name') as item_name,
json_value(items.item_info, 'strict $.item_num' returning bigint) as item_num,
json_value(items.item_info, 'strict $.core_cost' returning double) as core_cost
from user_daily a
cross join unnest(itemadd_forever) as items(item_info)
),

user_itemadd_daily as
(select month, channel, item_id, item_name, sum(item_num) as item_add, sum(core_cost) as core_cost
from user_itemadd_daily_unnest
group by 1, 2, 3, 4
)

select *
from user_itemadd_daily
;

# play
with user_daily as
(select date, date_trunc('month', date) as month,
role_id, channel,
corecost_play_array
from hive.dow_jp_w.dws_audit_daily_di
where part_date >= '2024-01-01'
),

user_play_daily_unnest as
(select date, month,
role_id, channel,
corecost_play_array,
json_extract_scalar(plays.play_info, '$.reason') as reason,
json_value(plays.play_info, 'strict $.corecost_play' returning double) as core_cost
from user_daily a
cross join unnest(corecost_play_array) as plays(play_info)
),

user_play_daily as
(select month, channel, reason, sum(core_cost) as core_cost
from user_play_daily_unnest
group by 1, 2, 3
)

select *
from user_play_daily
;