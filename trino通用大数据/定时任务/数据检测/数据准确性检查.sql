with money_daily_in_payment as (select part_date, cast(sum(money) as bigint) money
                                from hive.huntress_jp_r.dwd_gserver_payment_live
                                where part_date > '2024-01-01'
                                group by part_date),

     money_daily_in_user_daily as (select part_date, cast(sum(money) as bigint) money
                                   from hive.huntress_jp_w.dws_user_daily_di
                                   where part_date > '2024-01-01'
                                   group by part_date),
     money_diff as (select a.part_date, a.money as in_payment, b.money as in_user_daily, (a.money - b.money) as diff
                    from money_daily_in_payment a
                              join money_daily_in_user_daily b
                                       on a.part_date = b.part_date)
select * from money_diff where money_diff != 0;