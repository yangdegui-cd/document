--查询差异

with user_daily as
         (
         select
             date,
    date_trunc('month', date) as month,
    role_id,
    channel,
    money,
    core_gain_addmoney as core_gain,
    core_cost_addmoney as core_cost,
    reduce(itemadd_daily_array, 0, (s, x) ->
    s + coalesce((case when json_extract_scalar(x, '$.item_type') = 'once' then json_value(x, 'strict $.core_cost' returning double) else null end), 0),
    s -> s) as corecost_once,
    reduce(itemadd_daily_array, 0, (s, x) ->
    s + coalesce((case when json_extract_scalar(x, '$.item_type') = 'forever' then json_value(x, 'strict $.core_cost' returning double) else null end), 0),
    s -> s) as corecost_forever,
    corecost_play,
    itemadd_daily_array
from hive.huntress_jp_w.dws_audit_daily_di
where part_date >= '2024-05-01' and part_date <= '2024-05-31'
    )

SELECT
    *
FROM (
         SELECT
             *,
             COALESCE(core_cost, 0) - COALESCE(corecost_once, 0) - COALESCE(corecost_forever, 0) - COALESCE(corecost_play, 0) as difference
         FROM user_daily
         WHERE channel = '800002'
     ) AS subquery
WHERE difference > 10 OR difference < -10
LIMIT 10;

--某些用户当日花费了钻石 但是没有获取道具. 且不是玩法消费的钻石.  无法分摊出去