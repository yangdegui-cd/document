select
    count(*)
from
    hive.qkslg_om_r.dwd_gserver_payment_live
where
    money = money_ori * 10000000