
SELECT *
FROM stat2.card_probability_up_cycle
WHERE cardid= 5407
ORDER BY startdate DESC LIMIT 10;


SELECT DISTINCT cardid,
                ur_poolid poolid
FROM stat2.card_probability_up_cycle
WHERE ur_poolid>0
  AND up_to_ur>0
  AND startdate<='2025-01-01'
  AND enddate>='2025-01-21';



SELECT DISTINCT cardid,
                card_probability_up_cycle*
FROM stat2.card_probability_up_cycle
WHERE ur_poolid>0
  AND up_to_ur>0
  AND startdate<='2023-12-24'
  AND enddate>='2023-12-24';


SELECT *
FROM card_probability_up_cycle
WHERE ref_ur_activity_id = 1066;



SELECT *
FROM stat2.card_probability_up_cycle
WHERE zoneid=1
  AND (startdate BETWEEN '2025-01-01' AND '2025-01-21'
       OR enddate BETWEEN '2025-01-01' AND '2025-01-21'
       OR(startdate<='2025-01-01'
          AND enddate>='2025-01-21'));

select * from stat2.card_probability_up_wish_pool where (
  startdate between '2024-01-30' and '2024-03-26' 
  or enddate between '2024-01-30' and '2024-03-26'
  or(startdate<='2024-01-30' and enddate>='2024-03-26')) 

CREATE TABLE `card_probability_up_cycle` ( `id` int(11) NOT NULL DEFAULT '0', `zoneid` smallint(6) NOT NULL DEFAULT '0', `cardid` int(11) DEFAULT NULL, `startdate` date NOT NULL, `enddate` date NOT NULL, `orders` int(11) DEFAULT NULL, `poolid` int(11) NOT NULL, `reprint` tinyint(1) NOT NULL, `up_to_ur` tinyint(1) NOT NULL DEFAULT '0', `ur_poolid` int(11) DEFAULT NULL, `ur_reprint` tinyint(1) NOT NULL, `ref_ssr_activity_id` int(11) DEFAULT NULL, `ref_ssr_group_id` int(11) DEFAULT NULL, `ref_ssr_version` int(11) DEFAULT NULL, `ref_ur_activity_id` int(11) DEFAULT NULL, `ref_ur_group_id` int(11) DEFAULT NULL, `ref_ur_version` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 |
SELECT *
FROM card_probability_up_cycle
WHERE startdate='2023-12-27'
  AND enddate='2024-01-03'
  AND ref_ssr_activity_id='1013';

--删除错误数据
DELETE
FROM card_probability_up_cycle
WHERE startdate='2023-12-27'
  AND enddate='2024-01-03'
  AND ref_ssr_activity_id='1013';


-- 备份
INSERT
IGNORE INTO card_probability_up_cycle_bak231227(zoneid , cardid , startdate , enddate , orders , poolid , reprint , up_to_ur , ur_poolid , ur_reprint , ref_ssr_activity_id , ref_ssr_group_id , ref_ssr_version , ref_ur_activity_id , ref_ur_group_id , ref_ur_version)
SELECT zoneid ,
       cardid ,
       startdate ,
       enddate ,
       orders ,
       poolid ,
       reprint ,
       up_to_ur ,
       ur_poolid ,
       ur_reprint ,
       ref_ssr_activity_id ,
       ref_ssr_group_id ,
       ref_ssr_version ,
       ref_ur_activity_id ,
       ref_ur_group_id ,
       ref_ur_version
FROM stat2.card_probability_up_cycle;



INSERT INTO item_pay_user_rec_times_card_probability_up_star_stat(`pool_type`,`date`,`period`,`orders`,`reprint`,`uid`,`cardid`,`poolid`,`times`,`pay_dau`,`totalTimes`,`totalMoney`,`cardstar`,`starType`)


select * from item_pay_user_rec_times_card_probability_up_star_stat where orders = 396;



select * from item_pay_user_rec_times_card_probability_up_star_summary_stat where orders = 396;


select orders,period,pool_type,reprint,max(pay_dau) payDau,sum(times/totalTimes*totalMoney) totalMoney from stat2.item_pay_user_rec_times_card_probability_up_star_stat where 1=1 and orders = 396 
group by orders,period,pool_type,reprint order by orders desc;


select orders,period,pool_type,reprint,cardstar,starType,count( distinct uid) payDau,sum(times/totalTimes*totalMoney) totalMoney from stat2.item_pay_user_rec_times_card_probability_up_star_stat where 1=1  and orders = 396 
group by orders,period,pool_type,reprint,cardstar,starType;
