CREATE TABLE IF NOT EXISTS hive.qkslg2t_om_w.ads_user_retention_di (
    DATE DATE,
    install_date DATE,
    zone_id VARCHAR,
    channel VARCHAR,
    is_old_user INT,
    os VARCHAR,
    break_type VARCHAR,
    retention_day BIGINT,
    active_users BIGINT,
    pay_users BIGINT,
    newpay_users BIGINT,
    money DECIMAL(36, 2),
    money_rmb DECIMAL(36, 2),
    online_time BIGINT,
    payuser_ac BIGINT,
    money_ac DECIMAL(36, 2),
    moneyrmb_ac DECIMAL(36, 2),
    new_users BIGINT,
    part_date VARCHAR
) WITH(partitioned_by = ARRAY ['part_date']);

DELETE FROM
    hive.qkslg2t_om_w.ads_user_retention_di
WHERE
    part_date >= '2025-06-27'
    AND part_date <= '2025-06-28';

INSERT INTO
    hive.qkslg2t_om_w.ads_user_retention_di (
        DATE,
        install_date,
        zone_id,
        channel,
        is_old_user,
        os,
        break_type,
        retention_day,
        active_users,
        pay_users,
        newpay_users,
        money,
        money_rmb,
        online_time,
        payuser_ac,
        money_ac,
        moneyrmb_ac,
        new_users,
        part_date
    ) WITH user_daily AS(
        SELECT
            DATE,
            part_date,
            role_id,
            level_min,
            level_max,
            viplevel_min,
            viplevel_max,
            currency,
            money,
            online_time,
            is_old_user
        FROM
            hive.qkslg2t_om_w.dws_user_daily_di
    ),
    user_daily_join AS(
        SELECT
            a.date,
            a.part_date,
            a.role_id,
            a.is_old_user,
            a.level_min,
            a.level_max,
            a.viplevel_min,
            a.viplevel_max,
            a.money,
            a.money * 1.0000 AS money_rmb,
            a.online_time,
            b.install_date,
            DATE(b.lastlogin_ts) AS lastlogin_date,
            b.firstpay_date,
            b.firstpay_goodid,
            b.firstpay_level,
            b.zone_id,
            b.channel,
            b.os,
            (
                CASE WHEN b.install_date = b.firstpay_date THEN 'firstdate_break' WHEN b.firstpay_date IS NOT NULL THEN 'other_break' ELSE 'not_break' END
            ) AS break_type,
            date_diff('day', b.install_date, a.date) AS retention_day,
            date_diff('day', b.firstpay_date, a.date) AS pay_retention_day,
            date_diff('day', b.install_date, firstpay_date) AS firstpay_interval_days
        FROM
            user_daily a
            LEFT JOIN hive.qkslg2t_om_w.dws_user_info_di b ON a.role_id = b.role_id
            LEFT JOIN mysql_bi_r."gbsp-bi-bigdata".t_currency_rate z ON a.currency = z.currency
            AND date_format(a.date, '%Y-%m') = z.currency_time
        WHERE
            b.is_test IS NULL
            AND b.install_date >= DATE('2025-06-27')
            AND b.install_date < DATE('2025-06-28')
    ),
    retention_info AS(
        SELECT
            DATE,
            install_date,
            zone_id,
            channel,
            is_old_user,
            os,
            break_type,
            retention_day,
            COUNT(DISTINCT role_id) AS active_users,
            COUNT(
                DISTINCT (CASE WHEN money > 0 THEN role_id ELSE NULL END)
            ) AS pay_users,
            COUNT(
                DISTINCT (
                    CASE WHEN money > 0
                    AND pay_retention_day = 0 THEN role_id ELSE NULL END
                )
            ) AS newpay_users,
            SUM(money) AS money,
            SUM(money_rmb) AS money_rmb,
            SUM(online_time) AS online_time
        FROM
            user_daily_join
        GROUP BY
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8
    ),
    retention_all AS(
        SELECT
            install_date,
            zone_id,
            channel,
            is_old_user,
            os,
            break_type,
            SUM(
                CASE WHEN retention_day = 0 THEN active_users ELSE NULL END
            ) AS new_users
        FROM
            retention_info
        GROUP BY
            1,
            2,
            3,
            4,
            5,
            6
    ),
    data_cube AS(
        SELECT
            DISTINCT install_date,
            zone_id,
            channel,
            is_old_user,
            os,
            break_type,
            t.retention_day
        FROM
            retention_info
            CROSS JOIN UNNEST(SEQUENCE(0, 30, 1)) AS t(retention_day)
    ),
    retenion_info_cube AS(
        SELECT
            date_add('day', a.retention_day, a.install_date) AS DATE,
            a.install_date,
            a.zone_id,
            a.channel,
            a.is_old_user,
            a.os,
            a.break_type,
            a.retention_day,
            b.active_users,
            b.pay_users,
            b.newpay_users,
            b.money,
            b.money_rmb,
            b.online_time,
            c.new_users
        FROM
            data_cube a
            LEFT JOIN retention_info b ON a.install_date = b.install_date
            AND a.zone_id = b.zone_id
            AND a.channel = b.channel
            AND a.is_old_user = b.is_old_user
            AND a.os = b.os
            AND a.break_type = b.break_type
            AND a.retention_day = b.retention_day
            LEFT JOIN retention_all c ON a.install_date = c.install_date
            AND a.zone_id = c.zone_id
            AND a.channel = c.channel
            AND a.is_old_user = c.is_old_user
            AND a.os = c.os
            AND a.break_type = c.break_type
    )
SELECT
    DATE,
    install_date,
    zone_id,
    channel,
    is_old_user,
    os,
    break_type,
    retention_day,
    active_users,
    pay_users,
    newpay_users,
    money,
    money_rmb,
    online_time,
    SUM(newpay_users) OVER (
        PARTITION BY install_date,
        zone_id,
        channel,
        is_old_user,
        break_type,
        os
        ORDER BY
            retention_day ROWS BETWEEN unbounded preceding
            AND CURRENT ROW
    ) AS payuser_ac,
    SUM(money) OVER (
        PARTITION BY install_date,
        zone_id,
        channel,
        is_old_user,
        break_type,
        os
        ORDER BY
            retention_day ROWS BETWEEN unbounded preceding
            AND CURRENT ROW
    ) AS money_ac,
    SUM(money_rmb) OVER (
        PARTITION BY install_date,
        zone_id,
        channel,
        is_old_user,
        break_type,
        os
        ORDER BY
            retention_day ROWS BETWEEN unbounded preceding
            AND CURRENT ROW
    ) AS moneyrmb_ac,
    new_users,
    date_format(install_date, '%Y-%m-%d') AS part_date
FROM
    retenion_info_cube;