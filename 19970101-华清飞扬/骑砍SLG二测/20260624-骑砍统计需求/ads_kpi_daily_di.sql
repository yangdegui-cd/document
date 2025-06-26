create table if not exists hive.qkslg2t_om_w.ads_kpi_daily_di
(date date, 
zone_id varchar, 
channel varchar, 
active_users bigint, 
online_time bigint, 
new_users bigint, 
newuser_ac bigint, 
pay_users bigint, 
paid_users bigint, 
webpay_users bigint, 
install_pay bigint, 
newpay_users bigint, 
install_money decimal(36, 2), 
money decimal(36, 2), 
newpay_money decimal(36, 2), 
app_money decimal(36, 2), 
web_money decimal(36, 2), 
money_ac decimal(36, 2), 
pay_count bigint, 
app_count bigint, 
web_count bigint,
is_old_user int, 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.qkslg2t_om_w.ads_kpi_daily_di
where part_date >= '{yesterday}'
and part_date <= '{today}';

insert into hive.qkslg2t_om_w.ads_kpi_daily_di
(date, zone_id, channel,
active_users, online_time,
new_users, newuser_ac,
pay_users, paid_users, webpay_users, install_pay, newpay_users,
install_money, money, newpay_money, app_money, web_money, money_ac,
pay_count, app_count, web_count, is_old_user, part_date)

with user_daily as (
select date, part_date, role_id, online_time, money, web_money, app_money, pay_count, web_count, app_count, adid, is_old_user
from hive.qkslg2t_om_w.dws_user_daily_di a
where part_date <= '{today}'
),


user_daily_join as (
select a.date, a.part_date, a.role_id, a.online_time, a.money, a.web_money, a.app_money, b.money_ac, a.pay_count, a.web_count, a.app_count, a.is_old_user,
c.install_date, c.firstpay_date, c.channel, c.zone_id,
date_diff('day', c.install_date, a.date) as retention_day,
date_diff('day', c.firstpay_date, a.date) as pay_retention_day
from user_daily a
left join hive.qkslg2t_om_w.dws_user_daily_derive_di b
on a.role_id = b.role_id and a.part_date = b.part_date
left join hive.qkslg2t_om_w.dws_user_info_di c
on a.role_id = c.role_id
where c.is_test is null
),

daily_info as (
select
date, zone_id, channel, is_old_user,
count(distinct if(retention_day = 0, role_id, null)) as new_users,
count(distinct role_id) as active_users,
count(distinct if(money > 0, role_id, null)) as pay_users,
count(distinct if(money_ac > 0, role_id, null)) as paid_users,
count(distinct if(money > 0 and retention_day = 0 and pay_retention_day = 0, role_id, null)) as install_pay,
count(distinct if(money > 0 and pay_retention_day = 0, role_id, null)) as newpay_users,
sum(online_time) as online_time,
sum(money) as money,
sum(app_money) as app_money,
sum(web_money) as web_money,
sum(money_ac) as money_ac,
sum(if(money > 0 and retention_day = 0 and pay_retention_day = 0, money, 0)) as install_money,
sum(if(money > 0 and pay_retention_day = 0, money, 0)) as newpay_money,
sum(pay_count) as pay_count,
sum(app_count) as app_count,
sum(web_count) as web_count,
count(distinct if(web_money > 0, role_id, null)) as webpay_users
from user_daily_join
group by date, zone_id, channel, is_old_user
),

his_new as (
select distinct zone_id, channel
from hive.qkslg2t_om_w.dws_user_info_di
where zone_id is not null
and channel is not null
and trim(zone_id) != ''
and trim(channel) != ''
and zone_id != 'null'
and channel != 'null'
),

data_cube as (
select zone_id, channel, t.date, user.is_old_user
from his_new
cross join unnest(sequence(date '2025-06-23', date '{today}', interval '1' day)) as t(date)
cross join unnest(array[0, 1]) as user(is_old_user)
),

daily_info_cube as (
select
a.date, a.zone_id, a.channel, a.is_old_user,
coalesce(b.active_users, 0) as active_users,
coalesce(b.online_time, 0) as online_time,
coalesce(b.new_users, 0) as new_users,
coalesce(b.pay_users, 0) as pay_users,
coalesce(b.paid_users, 0) as paid_users,
coalesce(b.webpay_users, 0) as webpay_users,
coalesce(b.install_pay, 0) as install_pay,
coalesce(b.newpay_users, 0) as newpay_users,
coalesce(b.install_money, 0) as install_money,
coalesce(b.money, 0) as money,
coalesce(b.newpay_money, 0) as newpay_money,
coalesce(b.app_money, 0) as app_money,
coalesce(b.web_money, 0) as web_money,
coalesce(b.pay_count, 0) as pay_count,
coalesce(b.app_count, 0) as app_count,
coalesce(b.web_count, 0) as web_count
from data_cube a
left join daily_info b
on a.date = b.date and a.zone_id = b.zone_id and a.channel = b.channel and a.is_old_user = b.is_old_user
where a.date <= date '{today}'
),

filled_cube as (
select
date,
zone_id,
channel,
active_users,
online_time,
new_users,
sum(new_users) over(partition by zone_id, channel, is_old_user order by date rows between unbounded preceding and current row) as newuser_ac,
pay_users,
paid_users,
webpay_users,
install_pay,
newpay_users,
install_money,
money,
newpay_money,
app_money,
web_money,
sum(money) over(partition by zone_id, channel, is_old_user order by date rows between unbounded preceding and current row) as money_ac,
pay_count,
app_count,
web_count,
is_old_user,
date_format(date, '%Y-%m-%d') as part_date
from daily_info_cube
)

select *
from filled_cube
where date between date '{yesterday}' and date '{today}'
-- 更新版本 增加zone_id明细，不再使用ALL
;
