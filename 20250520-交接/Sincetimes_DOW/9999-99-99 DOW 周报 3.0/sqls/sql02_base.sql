/*
* @Author: dingyelen
* @Date:   2024-12-13 17:43:33
* @Last Modified by:   dingyelen
* @Last Modified time: 2024-12-17 10:23:05
*/

###
with ads_data as(
select date, sum(dau) as dau, sum(last7_dau) as last7_dau, sum(last30_dau) as last30_dau
from hive.dow_jpnew_w.ads_active_daily_di
where part_date >= $start_date and part_date <= $end_date
group by 1
), 

date_process_cal01 as(
select date, date_trunc('month', date) as month, 
last_day_of_month(date) as last_of_month, 
date_trunc('month', date_add('day', 6, date_trunc('week', date))) as natualweek_turn_month, 
date_trunc('week', date) as start_natualweek, 
date_add('day', 6, date_trunc('week', date)) as end_natualweek, 
7-day_of_week(date) as sunday_diff, 
day_of_week(date)-1 as monday_diff, 
dau, last7_dau, last30_dau
from ads_data
), 

date_process_cal02 as(
select date, month, 
last_of_month, natualweek_turn_month, 
start_natualweek, end_natualweek, 
sunday_diff, monday_diff, 
(case when date = month then date 
when date_add('day', -monday_diff, date) < month then month
else start_natualweek end) as start_week, 
(case when date = last_of_month then date 
when date_add('day', sunday_diff, date) > last_of_month then last_of_month
else date_add('day', sunday_diff, date) end) as end_week, 
dau, last7_dau, last30_dau
from date_process_cal01
) 

select date, month, 
natualweek_turn_month, start_natualweek, end_natualweek, 
start_week, end_week, 
concat(date_format(start_natualweek, '%m%d'), '-', date_format(end_natualweek, '%m%d')) as natual_week, 
concat(date_format(start_week, '%m%d'), '-', date_format(end_week, '%m%d')) as week, 
dau, last7_dau, last30_dau
from date_process_cal02;
###