CREATE TABLE IF NOT EXISTS hive.qkslg2t_om_w.ads_kpi_real_time_data (
    zone_id VARCHAR,
    os VARCHAR,
    channel VARCHAR,
    is_old_user INT,
    event_time TIMESTAMP,
    new_users BIGINT,
    dau BIGINT,
    retention1_users BIGINT,
    yesterday_newusers BIGINT,
    pay_users BIGINT,
    newpay_users BIGINT,
    install_pay BIGINT,
    install_money DOUBLE,
    money DOUBLE,
    web_money DOUBLE
);

DELETE FROM
    hive.qkslg2t_om_w.ads_kpi_real_time_data;

INSERT INTO
    hive.qkslg2t_om_w.ads_kpi_real_time_data (
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
WITH data_log2 AS (
    SELECT
        A.event_time,
        A.part_date,
        DATE(b.part_date) AS install_date,
        A.role_id,
        A.pay_source,
        A.money,
        CASE WHEN d.role_id IS NULL THEN 0 ELSE 1 END AS is_paid,
        FIRST_VALUE(A.zone_id) IGNORE NULLS OVER ( PARTITION BY A.role_id ORDER BY A.part_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS zone_id,
        FIRST_VALUE(A.os_name) IGNORE NULLS OVER ( PARTITION BY A.role_id ORDER BY A.part_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS os,
        FIRST_VALUE(A.channel) IGNORE NULLS OVER ( PARTITION BY A.role_id ORDER BY A.part_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS channel,
        FIRST_VALUE(A.adid) IGNORE NULLS OVER ( PARTITION BY A.role_id ORDER BY A.part_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS adid
    FROM hive.qkslg2t_om_r.dwd_merge_base_live A
    LEFT JOIN hive.qkslg2t_om_r.dwd_gserver_register_live b 
        ON A.role_id = b.role_id
    LEFT JOIN ( SELECT DISTINCT role_id FROM hive.qkslg2t_om_r.dwd_gserver_payment_live ) d 
        ON A.role_id = d.role_id
    WHERE A.part_date >= DATE_FORMAT( DATE_ADD('day', -1,DATE(AT_TIMEZONE(CURRENT_DATE, 'UTC'))), '%Y-%m-%d' )
),

data_log AS (
    SELECT
        A.*,
        CASE WHEN b.adid IS NULL THEN 0 ELSE 1 END AS is_old_user
    FROM data_log2 A
    LEFT JOIN hive.qkslg_om_w.dws_user_info_di b 
        ON A.adid = b.adid
),

today_agg AS (
    SELECT
        zone_id,
        os,
        channel,
        is_old_user,
        COUNT(DISTINCT( CASE WHEN install_date = DATE(AT_TIMEZONE(CURRENT_DATE, 'UTC')) OR install_date IS NULL THEN role_id ELSE NULL END )) AS new_users,
        COUNT(DISTINCT role_id) AS dau,
        COUNT(DISTINCT( CASE WHEN install_date = DATE_ADD('day', -1, DATE(AT_TIMEZONE(CURRENT_DATE, 'UTC'))) THEN role_id ELSE NULL END )) AS retention1_users,
        COUNT(DISTINCT( CASE WHEN money > 0 THEN role_id ELSE NULL END )) AS pay_users,
        COUNT(DISTINCT( CASE WHEN money > 0 AND ( is_paid = 0 OR is_paid IS NULL ) THEN role_id ELSE NULL END )) AS newpay_users,
        COUNT(DISTINCT( CASE WHEN money > 0 AND ( install_date =DATE(AT_TIMEZONE(CURRENT_DATE, 'UTC')) OR install_date IS NULL ) THEN role_id ELSE NULL END )) AS install_pay,
        SUM( CASE WHEN money > 0 AND ( is_paid = 0 OR is_paid IS NULL ) THEN money ELSE NULL END ) AS newpay_money,
        SUM( CASE WHEN money > 0 AND ( install_date =DATE(AT_TIMEZONE(CURRENT_DATE, 'UTC')) OR install_date IS NULL ) THEN money ELSE NULL END ) AS install_money,
        SUM(money) AS money,
        SUM( CASE WHEN pay_source = 'web' THEN money ELSE NULL END ) AS web_money
    FROM data_log
    WHERE part_date = DATE_FORMAT(DATE(AT_TIMEZONE(CURRENT_DATE, 'UTC')), '%Y-%m-%d')
    GROUP BY 1, 2, 3, 4
),

yesterday_agg AS (
    SELECT
        zone_id,
        os,
        channel,
        is_old_user,
        COUNT(DISTINCT ( CASE WHEN install_date = DATE_ADD('day', -1,DATE(AT_TIMEZONE(CURRENT_DATE, 'UTC'))) THEN role_id ELSE NULL END )) AS yesterday_newusers
    FROM data_log
    WHERE part_date = DATE_FORMAT( DATE_ADD('day', -1,DATE(AT_TIMEZONE(CURRENT_DATE, 'UTC'))), '%Y-%m-%d' )
    GROUP BY 1, 2, 3, 4
),


log_agg AS (
    SELECT MAX(event_time) AS event_time FROM data_log
)

SELECT
    COALESCE(A.zone_id, b.zone_id) AS zone_id,
    COALESCE(A.os, b.os) AS os,
    COALESCE(A.channel, b.channel) AS channel,
    COALESCE(A.is_old_user, b.is_old_user) AS is_old_user,
    C.event_time,
    COALESCE(A.new_users, 0) AS new_users,
    COALESCE(A.dau, 0) AS dau,
    COALESCE(A.retention1_users, 0) AS retention1_users,
    COALESCE(b.yesterday_newusers, 0) AS yesterday_newusers,
    COALESCE(A.pay_users, 0) AS pay_users,
    COALESCE(A.newpay_users, 0) AS newpay_users,
    COALESCE(A.install_pay, 0) AS install_pay,
    COALESCE(A.install_money, 0) AS install_money,
    COALESCE(A.money, 0) AS money,
    COALESCE(A.web_money, 0) AS web_money
FROM
    today_agg A FULL
    JOIN yesterday_agg b ON A.zone_id = b.zone_id
    AND A.os = b.os
    AND A.channel = b.channel
    AND A.is_old_user = b.is_old_user
    CROSS JOIN log_agg C