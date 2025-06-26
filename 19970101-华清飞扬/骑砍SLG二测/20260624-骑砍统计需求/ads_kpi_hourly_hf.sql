create table if not exists hive.qkslg2t_om_w.ads_hourly_income_summary
(
date date,
hour timestamp,
zone_id varchar,
channel varchar,
is_old_user int,
os varchar,
money decimal(36, 2),
pay_count bigint,
part_date varchar
)
with(
partitioned_by = array['part_date']
);

DELETE FROM hive.qkslg2t_om_w.ads_hourly_income_summary
WHERE part_date >= '{yesterday}'
and part_date <= '{today}';

insert into hive.qkslg2t_om_w.ads_hourly_income_summary
(date, hour, zone_id, channel, is_old_user ,money, pay_count, part_date)
select
date,
hour,
zone_id,
channel,
is_old_user,
sum(money) as money,
sum(pay_count) as pay_count,
part_date
from
hive.qkslg2t_om_w.dws_user_hourly_hi
WHERE part_date >= '{yesterday}'
and part_date <= '{today}'
and zone_id is not null
and channel is not null
and trim(zone_id) != ''
and trim(channel) != ''
group by
1,2,3,4,5,part_date
-- 更新版本 增加zone_id明细，不再使用ALL
;
