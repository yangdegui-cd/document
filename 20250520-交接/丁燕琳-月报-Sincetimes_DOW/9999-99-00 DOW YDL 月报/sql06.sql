-- trino
with month_tag as(
select month, role_id, pay_tag, is_test
from hive.dow_jpnew_w.dws_user_info_mi
where month = date('2025-03-01')
), 

user_tag as(
select role_id, vip_level, is_test
from hive.dow_jpnew_w.dws_user_info_di
), 

upgradestar_log as(
select date(part_date) as date, 
role_id, hero as hero_id, cast(substring(cast(torare as varchar), 3, 1) as bigint) as target_level
from hive.dow_jpnew_r.dwd_gserver_upgraderare_live
where part_date >= '2025-04-01'
and part_date <= '2025-04-30'
), 

upgradestar_group as(
select role_id, hero_id, 
count(*) as upgrade_count, 
max(target_level) as target_level
from upgradestar_log
group by 1, 2
), 

upgradestar_rank as(
select role_id, hero_id, upgrade_count, target_level, 
rank() over(partition by role_id order by upgrade_count) as upgrade_rank
from upgradestar_group
), 

hero_select as(
select * from upgradestar_rank
where upgrade_rank <= 18
)

select a.*, b.pay_tag, c.hero_cn
from hero_select a 
left join month_tag b
on a.role_id = b.role_id 
left join hive.dow_jpnew_w.dim_gserver_levelup_heroid c
on a.hero_id = cast(c.hero_id as bigint)
where is_test is null