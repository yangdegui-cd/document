drop table if exists hive.dow_jp_w.task_test_payment5;

create table hive.dow_jp_w.task_test_payment as 
 select part_date,role_id,sum(money) as money from hive.dow_jp_r.dwd_gserver_payment_live group by part_date,role_id;