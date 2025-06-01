

-- 删除数据
DELETE FROM mysql_shenji.zhanguo_jp_shenji_stat_base.audit_month_diamond WHERE month = SUBSTRING('{beginning_of_beforemonth}' FROM 1 FOR 7);
DELETE FROM mysql_shenji.zhanguo_jp_shenji_stat_base.audit_month_once_item WHERE month = SUBSTRING('{beginning_of_beforemonth}' FROM 1 FOR 7);
DELETE FROM mysql_shenji.zhanguo_jp_shenji_stat_base.audit_month_forever_item WHERE month = SUBSTRING('{beginning_of_beforemonth}' FROM 1 FOR 7);
DELETE FROM mysql_shenji.zhanguo_jp_shenji_stat_base.audit_month_play WHERE month = SUBSTRING('{beginning_of_beforemonth}' FROM 1 FOR 7);

-- 插入数据
INSERT INTO mysql_shenji.zhanguo_jp_shenji_stat_base.audit_month_diamond (
    month, 
    channel,
    money,
    diamond_add,
    diamond_cost,
    itemid_cost_diamond,
    item_rio,
    forever_item_cost,
    forever_item_rio,
    system_cost,
    system_cost_rio,
    wucha,
    wucha_rio
)
WITH user_daily AS (
    SELECT 
        date, 
        date_trunc('month', date) AS month,
        role_id, 
        channel,
        money, 
        core_gain_addmoney AS core_gain, 
        core_cost_addmoney AS core_cost,
        reduce(
            itemadd_daily_array, 0, 
            (s, x) -> s + COALESCE(
                CASE WHEN json_extract_scalar(x, '$.item_type') = 'once' 
                     THEN json_value(x, 'strict $.core_cost' RETURNING DOUBLE) 
                     ELSE NULL 
                END, 
                0
            ), 
            s -> s
        ) AS diamond_cost_once,
        reduce(
            itemadd_daily_array, 0, 
            (s, x) -> s + COALESCE(
                CASE WHEN json_extract_scalar(x, '$.item_type') = 'forever' 
                     THEN json_value(x, 'strict $.core_cost' RETURNING DOUBLE) 
                     ELSE NULL 
                END, 
                0
            ), 
            s -> s
        ) AS diamond_cost_forever,
        diamond_cost_play,
        itemadd_daily_array
    FROM hive.dow_jp_w.dws_audit_daily_di
    WHERE part_date >= '{beginning_of_beforemonth}' AND part_date < '{beginning_of_month}'
)
SELECT 
    month, 
    channel, 
    SUM(money),
    SUM(core_gain),
    SUM(core_cost),
    SUM(diamond_cost_once),
    CONCAT(CAST(ROUND((SUM(diamond_cost_once) / SUM(core_cost)) * 100, 4) AS VARCHAR), '%'),
    SUM(diamond_cost_forever) AS diamond_cost_forever,
    CONCAT(CAST(ROUND((SUM(diamond_cost_forever) / SUM(core_cost)) * 100, 4) AS VARCHAR), '%'),
    SUM(diamond_cost_play) AS diamond_cost_play,
    CONCAT(CAST(ROUND((SUM(diamond_cost_play) / SUM(core_cost)) * 100, 4) AS VARCHAR), '%'),
    SUM(core_gain) - SUM(diamond_cost_once) - SUM(diamond_cost_forever) - SUM(diamond_cost_play), 
    CONCAT(CAST(ROUND(((SUM(core_gain) - SUM(diamond_cost_once) - SUM(diamond_cost_forever) - SUM(diamond_cost_play)) / SUM(core_cost)) * 100, 4) AS VARCHAR), '%')
FROM user_daily
GROUP BY 1, 2;

-- 一次性道具消耗插入
INSERT INTO mysql_shenji.zhanguo_jp_shenji_stat_base.audit_month_once_item (
    month, 
    channel,
    itemid,
    itemname,
    itemid_add,
    itemid_cost,
    itemid_cost_diamond
)
WITH user_daily AS (
    SELECT 
        date, 
        date_trunc('month', date) AS month,
        role_id, 
        channel,
        filter(itemadd_daily_array, x -> json_extract_scalar(x, '$.item_type') = 'once') AS itemadd_once,
        filter(itemcost_daily_array, x -> json_extract_scalar(x, '$.item_type') = 'once') AS itemcost_once
    FROM hive.dow_jp_w.dws_audit_daily_di
    WHERE part_date >= '{beginning_of_beforemonth}' AND part_date < '{beginning_of_month}'
),
user_itemadd_daily_unnest AS (
    SELECT 
        date, 
        month,
        role_id, 
        channel,
        itemadd_once, 
        itemcost_once,
        json_extract_scalar(items.item_info, '$.item_id') AS item_id,
        json_extract_scalar(items.item_info, '$.item_name') AS item_name,
        json_value(items.item_info, 'strict $.item_num' RETURNING bigint) AS item_num,
        json_value(items.item_info, 'strict $.core_cost' RETURNING double) AS core_cost
    FROM user_daily a
    CROSS JOIN UNNEST(itemadd_once) AS items(item_info)
),
user_itemadd_daily AS (
    SELECT 
        month, 
        channel, 
        item_id, 
        item_name, 
        SUM(item_num) AS item_add, 
        SUM(core_cost) AS core_cost
    FROM user_itemadd_daily_unnest
    GROUP BY 1, 2, 3, 4
),
user_itemcost_daily_unnest AS (
    SELECT 
        date, 
        month,
        role_id, 
        channel,
        itemcost_once, 
        itemcost_once,
        json_extract_scalar(items.item_info, '$.item_id') AS item_id,
        json_value(items.item_info, 'strict $.item_num' RETURNING bigint) AS item_num
    FROM user_daily a
    CROSS JOIN UNNEST(itemcost_once) AS items(item_info)
),
user_itemcost_daily AS (
    SELECT 
        month, 
        channel, 
        item_id, 
        SUM(item_num) AS item_cost
    FROM user_itemcost_daily_unnest
    GROUP BY 1, 2, 3
)
SELECT 
    a.month,
    a.channel,
    a.item_id,
    a.item_name,
    a.item_add,
    b.item_cost,
    a.core_cost
FROM user_itemadd_daily a
LEFT JOIN user_itemcost_daily b
ON a.month = b.month 
AND a.channel = b.channel
AND a.item_id = b.item_id
ORDER BY 1;



-- 永久道具
INSERT INTO mysql_shenji.zhanguo_jp_shenji_stat_base.audit_month_forever_item (
    month, 
    channel,
    itemid,
    itemname,
    itemid_add_num,
    itemid_cost_diamond
)
WITH user_daily AS (
    SELECT 
        date, 
        date_trunc('month', date) AS month,
        role_id, 
        channel,
        filter(itemadd_daily_array, x -> json_extract_scalar(x, '$.item_type') = 'forever') AS itemadd_forever
    FROM hive.dow_jp_w.dws_audit_daily_di
    WHERE part_date >= '{beginning_of_beforemonth}' AND part_date < '{beginning_of_month}'
),
user_itemadd_daily_unnest AS (
    SELECT 
        date, 
        month,
        role_id, 
        channel,
        itemadd_forever,
        json_extract_scalar(items.item_info, '$.item_id') AS item_id,
        json_extract_scalar(items.item_info, '$.item_name') AS item_name,
        json_value(items.item_info, 'strict $.item_num' RETURNING bigint) AS item_num,
        json_value(items.item_info, 'strict $.core_cost' RETURNING double) AS core_cost
    FROM user_daily a
    CROSS JOIN UNNEST(itemadd_forever) AS items(item_info)
),
user_itemadd_daily AS (
    SELECT 
        month, 
        channel, 
        item_id, 
        item_name, 
        SUM(item_num) AS item_add, 
        SUM(core_cost) AS core_cost
    FROM user_itemadd_daily_unnest
    GROUP BY 1, 2, 3, 4
)
SELECT * FROM user_itemadd_daily;

-- 玩法消耗
INSERT INTO mysql_shenji.zhanguo_jp_shenji_stat_base.audit_month_play (
    month,
    channel,
    reason,
    core_cost
)
WITH user_daily AS (
    SELECT 
        date, 
        date_trunc('month', date) AS month,
        role_id, 
        channel,
        corecost_play_array
    FROM hive.dow_jp_w.dws_audit_daily_di
    WHERE part_date >= '{beginning_of_beforemonth}' AND part_date < '{beginning_of_month}'
),
user_play_daily_unnest AS (
    SELECT 
        date, 
        month,
        role_id, 
        channel,
        corecost_play_array,
        json_extract_scalar(plays.play_info, '$.reason') AS reason,
        json_value(plays.play_info, 'strict $.corecost_play' RETURNING double) AS core_cost
    FROM user_daily a
    CROSS JOIN UNNEST(corecost_play_array) AS plays(play_info)
),
user_play_daily AS (
    SELECT 
        month, 
        channel, 
        reason, 
        SUM(core_cost) AS core_cost
    FROM user_play_daily_unnest
    GROUP BY 1, 2, 3
)
SELECT * FROM user_play_daily;
