
-- 验证收入差异,  如果有差异.  数据有误
with money_daily_in_payment
       as (select part_date, cast(sum(money) as bigint) money
           from hive.dow_jp_r.dwd_gserver_payment_live
           where part_date = DATE_FORMAT(CURRENT_DATE - INTERVAL '1' DAY, '%Y-%m-%d')
           group by part_date),

   money_daily_in_user_daily
       as (select part_date, cast(sum(money) as bigint) money
           from hive.dow_jp_w.dws_user_daily_di
           where part_date = DATE_FORMAT(CURRENT_DATE - INTERVAL '1' DAY, '%Y-%m-%d')
           group by part_date),
   money_diff
       as (select a.part_date as date,
                  a.money as in_payment_money_total, 
                  b.money as in_user_daily_money_total, 
                  (COALESCE(a.money, 0) - COALESCE(b.money, 0)) as diff_money
           from money_daily_in_payment a
           join money_daily_in_user_daily b
                on a.part_date = b.part_date)

select abs(COALESCE(diff_money, 0)) from money_diff



-- 验证是否有重复的RoleID, 如果有, 数据有误
SELECT role_id, count(1) FROM hive.dow_jp_w.dws_user_daily_di
where part_date = DATE_FORMAT(CURRENT_DATE - INTERVAL '1' DAY, '%Y-%m-%d')
GROUP BY 1
HAVING count(1) > 1