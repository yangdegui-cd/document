 
-- 如果一个role_id有大于一条的数据,  那么说明数据有误

SELECT role_id, count(1) FROM hive.dow_jp_w.dws_user_info_di
GROUP BY 1
HAVING count(1) > 1;


-- 如果user_info和dwd_merge_base_live的用户数.  如果误差达到一定程度. 认为数据有误. 数据结果为 x/千. 


WITH user_info_count AS (
    SELECT COUNT(1) AS count
    FROM hive.dow_jp_w.dws_user_info_di
),
merge_base_count AS (
    SELECT COUNT(DISTINCT role_id) AS count
    FROM hive.dow_jp_r.dwd_merge_base_live
)
SELECT ABS(user_info_count.count - merge_base_count.count) / user_info_count.count * 1000 AS result
FROM user_info_count, merge_base_count
