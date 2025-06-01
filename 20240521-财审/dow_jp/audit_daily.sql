#【arguments】#
start_date = today
end_date = today + timedelta(days=-1)
#【arguments】#

/*
1. 确定币种与钻石的汇率【修改core_gain_addmoney、core_cost_addmoney】
2. 确定道具定价（道具id、道具名称、道具类型、道具单价），参考文件【dim_gserver_additem_itemid.csv-燕琳维护】
3. user_daily_base付费用户的选取及计算，实现2点：用户首次付费前剩余的钻石计算为用户首次获取的数据；用户充值的钱根据汇率折算成钻石产销
4. 确定玩法reason【修改event_corecost_play子查询中的reason内容】，保留reason、cost_num
5. event_itemadd_base选取用户道具获数据，需要保留item_id、item_num, 同时根据item_id匹配道具的信息（备注item_id作为唯一匹配键）
6. event_itemcost_base获取用户道具消耗数据
7. user_daily_final合并所有数据
*/

###
create table if not exists hive.dow_jp_w.dws_audit_daily_di
(date date,
role_id varchar,
channel varchar,
money bigint,
money_rmb double,
core_remain bigint,
core_gain bigint,
core_cost bigint,
        core_gain_addmoney bigint,
core_cost_addmoney bigint,
core_end bigint,
install_date date,
firstpay_date date,
corecost_play bigint,
corecost_play_array array(varchar),
itemadd_daily_array array(varchar),
itemcost_daily_array array(varchar),
part_date varchar
)
with(
    format = 'orc',
    transactional = true,
    partitioned_by = array['part_date']
);


delete from hive.dow_jp_w.dws_audit_daily_di where part_date >= '{yesterday}' and part_date <= '{today}';

insert into hive.dow_jp_w.dws_audit_daily_di
(date, role_id, channel, money, money_rmb, core_remain, core_gain, core_cost, core_gain_addmoney, core_cost_addmoney, core_end, 
install_date, firstpay_date, corecost_play, corecost_play_array, itemadd_daily_array, itemcost_daily_array,
part_date)

-- 第一步，选取用户
with user_daily as
(select date, role_id, money, money_rmb, 
coalesce(core_end, 0) - coalesce(core_gain, 0) + coalesce(core_cost, 0) as core_remain, 
core_gain, core_cost, core_end
from hive.dow_jp_w.dws_user_daily_di
where part_date >= '{yesterday}' and part_date <= '{today}'
),

user_info as
(select role_id, channel, install_date, firstpay_date
from hive.dow_jp_w.dws_user_info_di a
where exists (select * from user_daily b where a.role_id = b.role_id)
and firstpay_date is not null
),

user_daily_base as
(select date, a.role_id, b.channel, money, money_rmb, core_remain, core_gain, core_cost,
(case when date = b.firstpay_date then coalesce(core_remain, 0) + coalesce(core_gain, 0) + coalesce(money, 0) * 0.56
else coalesce(core_gain, 0) + coalesce(money, 0) * 0.56 end) as core_gain_addmoney,  -- 用户付费首日，充值前一日的剩余计算为第一条新增日志; 充值金额按照汇率增加
(coalesce(core_cost, 0) + coalesce(money, 0) * 0.56) as core_cost_addmoney,  -- 充值金额按照汇率增加
core_end,
b.install_date, b.firstpay_date
from user_daily a
inner join user_info b
on a.role_id = b.role_id
where a.date >= b.firstpay_date
),

-- 第二步，玩法消耗
event_corecost_play as
(select event_time, date(event_time) as date, date_trunc('month', event_time) as month,
role_id,
reason as reason, 
diamond_num as core_num, 
part_date
from hive.dow_jp_r.dwd_gserver_costgold_live
where part_date >= '{yesterday}' and part_date <= '{today}'
and reason not in ('1', '28', '180', '185', '187', '197', '199', '218', '231', '232', '333', '336', '354', '378', '401', '402', '403', '404', '405', '406', '407', '408', '409', '410', '411', '413', '414', '421', '431', '432', '433', '434', '460', '471', '473', '604', '609', '643', '640')
),

user_corecost_play_agg as
(select date, role_id, reason, sum(core_num) as corecost_play
from event_corecost_play
group by 1, 2, 3
),

user_corecost_play_daily as
(select date, role_id, sum(corecost_play) as corecost_play, array_agg(json_object('reason': reason, 'corecost_play': corecost_play)) as corecost_play_array
from user_corecost_play_agg
group by 1, 2 
),

-- 第三步，道具获得
event_itemadd_base as
(select event_time, date(event_time) as date, date_trunc('month', event_time) as month,
role_id,
channel, zone_id, 
event_name,
a.item_id as item_id,
reason as reason, 
item_add as item_num, core_values as core_cost, 
b.item_type, 
(case when b.is_special != '1' then b.gold_value else 0 end) as item_price,
 b.item_name,
part_date
from hive.dow_jp_r.dwd_gserver_additem_live a
left join hive.dow_jp_w.dim_gserver_additem_itemid b
on a.item_id = b.item_id
where part_date >= '{yesterday}' and part_date <= '{today}'
),

itemadd_agg as
(select date, role_id, item_id, item_name, item_type, item_price, sum(item_num) as item_num
from  event_itemadd_base
group by 1, 2, 3, 4, 5, 6
),

itemadd_daily as
(select date, role_id, sum(item_price * item_num) as price_all,
array_agg(json_object('item_type': item_type, 'item_id': item_id, 'item_num': item_num,
    'item_price': item_price, 'price_all':  item_price * item_num, 'item_name': item_name)) as itemadd_daily
from itemadd_agg
group by 1, 2
),

-- 第四步， 道具消耗
event_itemcost_base as
(select event_time, date(event_time) as date, date_trunc('month', event_time) as month,
role_id,
channel, zone_id, 
event_name,
a.item_id as item_id,
reason as reason, 
item_cost as item_num, 
b.item_type, 
(case when b.is_special != '1' then b.gold_value else 0 end) as item_price,
 b.item_name,
part_date
from hive.dow_jp_r.dwd_gserver_costitem_live a
left join hive.dow_jp_w.dim_gserver_additem_itemid b
on a.item_id = b.item_id
where part_date >= '{yesterday}' and part_date <= '{today}'
),

itemcost_agg as
(select date, role_id, item_id, item_name, item_type, item_price, sum(item_num) as item_num
from  event_itemcost_base
group by 1, 2, 3, 4, 5, 6
),

itemcost_daily as
(select date, role_id, 
array_agg(json_object('item_type': item_type, 'item_id': item_id, 'item_num': item_num,
    'item_price': item_price, 'item_name': item_name)) as itemcost_daily
from itemcost_agg
group by 1, 2
),

-- 第五步， 合并结果
user_daily_final as
(select a.*,
b.corecost_play, b.corecost_play_array,
transform(c.itemadd_daily, 
x -> json_object(
    'item_type': json_extract_scalar(x, '$.item_type'),
    'item_id': json_extract_scalar(x, '$.item_id'),
    'item_num': json_extract_scalar(x, '$.item_num'),
    'item_price': json_extract_scalar(x, '$.item_price'),
    'price_all': json_extract_scalar(x, '$.price_all'),
    'core_cost': try_cast((a.core_cost_addmoney - coalesce(b.corecost_play, 0)) / price_all * json_value(x, 'strict $.price_all' returning double) as decimal(10, 2)),
    'item_name': json_extract_scalar(x, '$.item_name')
)) as itemadd_daily_array,
d.itemcost_daily as itemcost_daily_array
from user_daily_base a
left join user_corecost_play_daily b
on a.date = b.date and a.role_id = b.role_id
left join itemadd_daily c
on a.date = c.date and a.role_id = c.role_id
left join itemcost_daily d
on a.date = d.date and a.role_id = d.role_id
)

select *,
cast(date as varchar) as part_date
from user_daily_final
-- where role_id = '16574279081667'
-- order by date
;
###