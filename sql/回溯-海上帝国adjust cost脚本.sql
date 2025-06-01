WITH AdjustCost AS (
    SELECT
        CASE
            WHEN network like '%Facebook%' THEN 'Facebook'
            WHEN network like '%Google Ads%' THEN 'Google Ads ACI'
            WHEN network = 'TikTok for Business (Ad Spend)'
            and os_name = 'android' THEN 'non-TikTok-AND'
            WHEN network = 'TikTok for Business (Ad Spend)'
            and os_name = 'ios' THEN 'non-TikTok-iOS'
            ELSE network
        END network_name,
        campaign_id,
        adgroup_id,
        os_name,
        "$part_date" install_date,
        country_code country,
        sum(CAST(cost AS DECIMAL(10, 4))) cost
    FROM
        ta.v_event_36
    WHERE
        "$part_event" = 'adjust_cost'
        AND "$part_date" >= '2023-01-01'
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6
),
UserInfo AS (
    SELECT
        "#user_id",
        "#account_id",
        network_name,
        campaign_id,
        adgroup_id,
        country,
        os_name,
        campaign_name,
        adgroup_name,
        creative_name
    FROM
        ta.v_user_36
    WHERE
        "#account_id" IS NOT NULL
),
UserInstallTag AS (
    SELECT
        "#user_id",
        max(
            case
                when cluster_name = 'da_install_ts' then tag_value_tm
                else null
            end
        ) as install_at
    FROM
        ta.user_result_cluster_36
    GROUP BY
        1
),
UserInfoWithInstallDate AS (
    SELECT
        UserInfo.*,
        b.install_at,
        date_format(b.install_at, '%Y-%m-%d') AS install_date
    FROM
        UserInfo
        LEFT JOIN UserInstallTag b ON UserInfo."#user_id" = b."#user_id"
),
AdjustCostAvg AS (
    SELECT
        c.network_name,
        c.campaign_id,
        c.adgroup_id,
        c.install_date,
        c.country,
        c.os_name,
        c.cost,
        d_user.user_count,
        noc_user.user_count noc_user_count,
        no_user.user_count no_user_count,
        n_user.user_count n_user_count,
        (
            CASE
                WHEN d_user.user_count > 0 THEN c.cost / d_user.user_count
                ELSE 0
            END
        ) avg_cost,
        (
            CASE
                WHEN COALESCE(d_user.user_count, 0) = 0
                AND COALESCE(noc_user.user_count, 0) > 0 THEN c.cost / noc_user.user_count
                ELSE 0
            END
        ) noc_avg_cost,
        (
            CASE
                WHEN COALESCE(d_user.user_count, 0) = 0
                AND COALESCE(noc_user.user_count, 0) = 0
                AND COALESCE(no_user.user_count, 0) > 0 THEN c.cost / no_user.user_count
                ELSE 0
            END
        ) no_avg_cost,
        (
            CASE
                WHEN COALESCE(d_user.user_count, 0) = 0
                AND COALESCE(noc_user.user_count, 0) = 0
                AND COALESCE(no_user.user_count, 0) = 0
                AND COALESCE(n_user.user_count, 0) > 0 THEN c.cost / n_user.user_count
                ELSE 0
            END
        ) n_avg_cost,
        (
            CASE
                WHEN COALESCE(d_user.user_count, 0) = 0
                AND COALESCE(noc_user.user_count, 0) = 0
                AND COALESCE(no_user.user_count, 0) = 0
                AND COALESCE(n_user.user_count, 0) = 0 THEN c.cost
                ELSE 0
            END
        ) not_match_cost
    FROM
        AdjustCost c
        LEFT JOIN (
            SELECT
                network_name,
                campaign_id,
                adgroup_id,
                install_date,
                country,
                os_name,
                count(*) user_count
            FROM
                UserInfoWithInstallDate
            GROUP BY
                1,
                2,
                3,
                4,
                5,
                6
        ) d_user ON c.network_name = TRIM(d_user.network_name)
        AND c.campaign_id = d_user.campaign_id
        AND c.adgroup_id = d_user.adgroup_id
        AND c.os_name = d_user.os_name
        AND c.install_date = d_user.install_date
        AND c.country = d_user.country
        LEFT JOIN (
            SELECT
                network_name,
                install_date,
                os_name,
                country,
                count(*) user_count
            FROM
                UserInfoWithInstallDate
            GROUP BY
                1,
                2,
                3,
                4
        ) noc_user ON c.network_name = noc_user.network_name
        AND c.install_date = noc_user.install_date
        AND c.country = noc_user.country
        AND c.os_name = noc_user.os_name
        LEFT JOIN (
            SELECT
                network_name,
                os_name,
                install_date,
                count(*) user_count
            FROM
                UserInfoWithInstallDate
            GROUP BY
                1,
                2,
                3
        ) no_user ON c.network_name = no_user.network_name
        AND c.install_date = no_user.install_date
        AND c.os_name = no_user.os_name
        LEFT JOIN (
            SELECT
                network_name,
                install_date,
                count(*) user_count
            FROM
                UserInfoWithInstallDate
            GROUP BY
                1,
                2
        ) n_user ON c.network_name = n_user.network_name
        AND c.install_date = n_user.install_date
),
MatchResultTable AS (
    SELECT
        u."#account_id",
        null as "#distinct_id",
        date_format(u.install_at, '%Y-%m-%d %H::%s') as "#time",
        'adjust_cost_detail' as "#event_name",
        u.install_date,
        u.network_name,
        u.campaign_id,
        u.adgroup_id,
        u.country,
        u.os_name,
        u.campaign_name,
        u.adgroup_name,
        u.creative_name,
        CONCAT(
            u."#account_id",
            '',
            u.install_date,
            u.network_name,
            COALESCE(u.campaign_id, ''),
            COALESCE(u.adgroup_id, ''),
            COALESCE(u.country, ''),
            COALESCE(u.os_name, '')
        ) as "#event_id",
        COALESCE(aca.avg_cost, 0) as ad_cost,
        COALESCE(aca_noc.ad_cost_noc_apportion, 0) as ad_cost_noc_apportion,
        COALESCE(aca_no.ad_cost_no_apportion, 0) as ad_cost_no_apportion,
        COALESCE(aca_n.ad_cost_n_apportion, 0) as ad_cost_n_apportion
    FROM
        UserInfoWithInstallDate u
        LEFT JOIN AdjustCostAvg aca ON u.network_name = aca.network_name
        AND u.campaign_id = aca.campaign_id
        AND u.adgroup_id = aca.adgroup_id
        AND u.install_date = aca.install_date
        AND u.os_name = aca.os_name
        AND u.country = aca.country
        LEFT JOIN (
            SELECT
                network_name,
                install_date,
                os_name,
                country,
                sum(noc_avg_cost) ad_cost_noc_apportion
            FROM
                AdjustCostAvg aca
            WHERE
                noc_avg_cost > 0
            GROUP BY
                1,
                2,
                3,
                4
        ) aca_noc ON u.network_name = aca_noc.network_name
        AND u.install_date = aca_noc.install_date
        AND u.os_name = aca_noc.os_name
        AND u.country = aca_noc.country
        LEFT JOIN (
            SELECT
                network_name,
                install_date,
                os_name,
                sum(no_avg_cost) ad_cost_no_apportion
            FROM
                AdjustCostAvg acan
            WHERE
                no_avg_cost > 0
            GROUP BY
                1,
                2,
                3
        ) aca_no ON u.network_name = aca_no.network_name
        and u.install_date = aca_no.install_date
        AND u.os_name = aca_no.os_name
        LEFT JOIN (
            SELECT
                network_name,
                install_date,
                sum(n_avg_cost) ad_cost_n_apportion
            FROM
                AdjustCostAvg aca
            WHERE
                n_avg_cost > 0
            GROUP BY
                1,
                2
        ) aca_n ON u.network_name = aca_n.network_name
        AND u.install_date = aca_n.install_date
),
NotMatchResultTable AS (
    SELECT
        null as "#account_id",
        'adjust_not_match_cost' as "#distinct_id",
        CONCAT(install_date, ' 00:00:00') as "#time",
        'adjust_cost_detail' as "#event_name",
        install_date,
        network_name,
        campaign_id,
        adgroup_id,
        country,
        os_name,
        '' as campaign_name,
        '' as adgroup_name,
        '' as creative_name,
        CONCAT(
            '',
            'adjust_not_match_cost',
            install_date,
            network_name,
            COALESCE(campaign_id, ''),
            COALESCE(adgroup_id, ''),
            COALESCE(country, ''),
            COALESCE(os_name, '')
        ) as "#event_id",
        not_match_cost as ad_cost,
        0 as ad_cost_noc_apportion,
        0 as ad_cost_no_apportion,
        0 as ad_cost_n_apportion
    FROM
        AdjustCostAvg a
    WHERE
        not_match_cost > 0
),
ResultTable AS (
    SELECT
        *
    FROM
        MatchResultTable
    UNION
    ALL
    SELECT
        *
    FROM
        NotMatchResultTable
) -- 结果花费求和
-- select sum(ad_cost) + sum(other_network_ad_cost) from ResultTable
-- 平均花费求和验证
-- select sum(case
--               when COALESCE(user_count, 0) = 0  then network_user_count * network_avg_cost
--               else user_count * avg_cost end) adjust_total
-- from AdjustCostAvg
-- 实际花费总和
-- select sum(CAST(cost AS DECIMAL(10, 2))) cost  FROM ta.v_event_36 WHERE "$part_event" = 'adjust_cost' AND "$part_date" >= '2023-01-01'
-- 数据回溯sql, 第一次执行, 手动执行, 确保 ad_cost, other_network_ad_cost 能成功录入,  保证事件中存在这两个KEY,
-- 为之后每日定时回溯脚本做准备
select
    *
from
    ResultTable -- 每日定时回溯脚本,  只更新 ad_cost, other_network_ad_cost有变化的数据
    -- select ResultTable.*
    --  from ResultTable
    --           left join (select * from ta.v_event_36 where "$part_event" = 'adjust_cost_detail') e
    --                     on ResultTable."#event_id" = e."#event_id"
    --  where e."#event_id" is null
    --  or e.ad_cost != ResultTable.ad_cost 
    --  or e.ad_cost_noc_apportion != ResultTable.ad_cost_noc_apportion 
    --  or e.ad_cost_no_apportion != ResultTable.ad_cost_no_apportion 
    --  or e.ad_cost_n_apportion != ResultTable.ad_cost_n_apportion