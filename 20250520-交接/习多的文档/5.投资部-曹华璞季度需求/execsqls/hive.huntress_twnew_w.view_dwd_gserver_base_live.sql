with currency as
(select currency_time as currency_month, currency,
rate as exchange_rate
from mysql_bi_r."gbsp-bi-bigdata".t_currency_rate
where currency_time >= substr('2024-01-01', 1, 7)
and currency_time <= substr('2025-01-14', 1, 7)
),
event_base as(
select part_date, event_name, event_time,
date(event_time) as date,
date_trunc('month', event_time) as month,
role_id, open_id,
cast(payment_itemid as varchar) as good_id,
a.currency, money, b.exchange_rate,
(case when 'huntress_twnew' in ('tank_cnnew') and substr(a.currency, 1, 3) = 'CNY' then money
when 'huntress_twnew' in ('warship_cnnew', 'warship2_cnnew', 'modernship_cnnew') and substr(a.currency, 1, 3) = 'CNY' then money * 0.01
when 'huntress_twnew' in ('huntress_jpnew', 'huntress_krnew', 'huntress_twnew', 'warship_jpnew', 'tank_jpnew')  then money * b.exchange_rate
when 'huntress_twnew' in ('huntress_omnew', 'dow_jpnew', 'warship2_asianew') then money * b.exchange_rate * 0.01
else money * b.exchange_rate end) as money_rmb,
online_time
from hive.huntress_twnew_w.view_dwd_gserver_base_live a
left join currency b
on substr(part_date, 1, 7) = b.currency_month
and substr(a.currency, 1, 3) = b.currency
where part_date >= '2024-01-01'
and part_date <= '2025-01-14'
and 1 = 1
),
user_daily_agg as
(select month,
count(distinct open_id) as users,
round(sum(money_rmb), 2) as money_rmb
from event_base
group by 1
),
start_date as
(select min(month) as start_date
from event_base
)
select *,
date_diff('month', start_date, month) - row_number() over (order by month) + 1 as continue_state
from user_daily_agg a
cross join start_date b
order by 1