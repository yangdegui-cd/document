CREATE Table IF NOT EXISTS hive.qkslg2t_om_w.ads_kpi_real_time_data (
    zone_id VARCHAR,
    os VARCHAR,
    channel VARCHAR,
    is_old_user int,
    event_time timestamp,
    new_users bigint,
    dau bigint,
    retention1_users bigint,
    yesterday_newusers bigint,
    pay_users bigint,
    newpay_users bigint,
    install_pay bigint,
    install_money double,
    money double,
    web_money double
);

DELETE FROM hive.qkslg2t_om_w.ads_kpi_real_time_data;

INSERT INTO hive.qkslg2t_om_w.ads_kpi_real_time_data(
    zone_id, 
    os, 
    channel, 
    is_old_user, 
    event_time, 
    new_users, 
    dau, 
    retention1_users, 
    yesterday_newusers, 
    pay_users, 
    newpay_users, 
    install_pay, 
    install_money, 
    money, 
    web_money
)

WITH data_log AS (
SELECT 
    a.event_time, 
    a.part_date, 
    b.install_date, 
    a.role_id, 
    (CASE WHEN b.firstpay_date IS NOT NULL AND b.firstpay_date < current_date THEN 1 ELSE 0 END) AS is_paid, 
    a.pay_source, 
    a.money,
    first_value(a.zone_id) ignore nulls over(partition by a.role_id order by a.part_date rows between unbounded preceding and unbounded following) as zone_id,
    first_value(a.os_name) ignore nulls over(partition by a.role_id order by a.part_date rows between unbounded preceding and unbounded following) as os,
    first_value(a.channel) ignore nulls over(partition by a.role_id order by a.part_date rows between unbounded preceding and unbounded following) as channel,
    CASE WHEN c.adid IS NULL THEN 0 ELSE 1 END AS is_old_user
FROM hive.qkslg2t_om_r.dwd_merge_base_live a 
LEFT JOIN hive.qkslg2t_om_w.dws_user_info_di b 
    ON a.role_id = b.role_id 
LEFT JOIN (SELECT DISTINCT adid FROM hive.qkslg_om_w.dws_user_info_di) c
    ON a.adid = c.adid
WHERE 
    a.part_date >= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
    AND b.is_test is null
), 

today_agg as (
SELECT
    zone_id, os,channel,is_old_user,
    approx_distinct(case when install_date = current_date or install_date is null then role_id else null end) as new_users, 
    approx_distinct(role_id) as dau, 
    approx_distinct(case when install_date = date_add('day', -1, current_date) then role_id else null end) as retention1_users, 
    approx_distinct(case when money > 0 then role_id else null end) as pay_users, 
    approx_distinct(case when money > 0 and (is_paid = 0 or is_paid is null) then role_id else null end) as newpay_users, 
    approx_distinct(case when money > 0 and (install_date = current_date or install_date is null) then role_id else null end) as install_pay, 
    sum(case when money > 0 and (is_paid = 0 or is_paid is null) then money else null end) as newpay_money, 
    sum(case when money > 0 and (install_date = current_date or install_date is null) then money else null end) as install_money, 
    sum(money) as money, 
    sum(case when pay_source = 'web' then money else null end) as web_money 
FROM data_log
where  part_date = date_format(current_date, '%Y-%m-%d')
GROUP BY 1,2,3,4
), 

yesterday_agg AS(
SELECT 
    zone_id, os,channel,is_old_user,
    approx_distinct(case when install_date = date_add('day', -1, current_date) then role_id else null end) as yesterday_newusers
FROM data_log
WHERE part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
GROUP BY 1,2,3,4
), 

log_agg as(
    SELECT max(event_time) AS event_time
    FROM data_log
)

SELECT
COALESCE(a.zone_id, b.zone_id) AS zone_id,
COALESCE(a.os, b.os) AS os,
COALESCE(a.channel, b.channel) AS channel,
COALESCE(a.is_old_user, b.is_old_user) AS is_old_user,
c.event_time, 
COALESCE(a.new_users, 0) AS new_users, 
COALESCE(a.dau, 0) AS dau,
COALESCE(a.retention1_users, 0) AS retention1_users,
COALESCE(b.yesterday_newusers,0) AS yesterday_newusers, 
COALESCE(a.pay_users, 0) AS pay_users,
COALESCE(a.newpay_users, 0) AS newpay_users,
COALESCE(a.install_pay, 0) AS install_pay,
COALESCE(a.install_money, 0) AS install_money,
COALESCE(a.money, 0) AS money,
COALESCE(a.web_money, 0) AS web_money
from today_agg a
FULL JOIN yesterday_agg b
ON a.zone_id = b.zone_id
AND a.os = b.os
AND a.channel = b.channel
AND a.is_old_user = b.is_old_user
CROSS JOIN log_agg c