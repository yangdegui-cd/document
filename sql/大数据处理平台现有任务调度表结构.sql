-----------流程表---------------
CREATE TABLE `bd_flow_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `flow_id` varchar(36) NOT NULL,
  `flow_name` varchar(128) DEFAULT NULL,
  `status` int(1) NOT NULL DEFAULT '0',
  `create_user` varchar(128) DEFAULT NULL,
  `group_code` varchar(128) DEFAULT '',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `deploy_time` datetime DEFAULT NULL,
  `update_version` int(6) NOT NULL DEFAULT '0',
  `deploy_version` int(6) NOT NULL DEFAULT '0',
  `desc` varchar(4096) DEFAULT NULL,
  `flow_code` varchar(128) NOT NULL DEFAULT '',
  `role_flag` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=960 DEFAULT CHARSET=utf8
+-----+----------------------------------+------------------+--------+-------------+-----------------------------+---------------------+---------------------+---------------------+----------------+----------------+------+------------------------+-----------+
| id  | flow_id                          | flow_name        | status | create_user | group_code                  | create_time         | update_time         | deploy_time         | update_version | deploy_version | desc | flow_code              | role_flag |
+-----+----------------------------------+------------------+--------+-------------+-----------------------------+---------------------+---------------------+---------------------+----------------+----------------+------+------------------------+-----------+
| 959 | 53e5389b39844f7ca70f1167c08b3d82 | 转换json测试     |      1 | 12          | flow_hive_import_group_test | 2023-09-07 14:46:35 | 2023-09-14 18:03:26 | 2023-09-14 18:03:26 |             30 |             30 | NULL | hive_json_convert_test |         0 |
+-----+----------------------------------+------------------+--------+-------------+-----------------------------+---------------------+---------------------+---------------------+----------------+----------------+------+------------------------+-----------+
-----------任务表--------
CREATE TABLE `bd_task_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `flow_id` bigint(20) NOT NULL,
  `task_name` varchar(128) NOT NULL,
  `task_script` varchar(1024) NOT NULL,
  `task_params` varchar(8192) DEFAULT NULL,
  `task_type` int(1) NOT NULL DEFAULT '0',
  `call_type` int(1) NOT NULL DEFAULT '0',
  `call_quartz` varchar(36) NOT NULL DEFAULT '0',
  `data_time` datetime DEFAULT NULL,
  `valid` int(1) NOT NULL DEFAULT '0',
  `last_status` int(2) DEFAULT '0',
  `last_call` datetime DEFAULT NULL,
  `schedule_start` datetime DEFAULT NULL,
  `schedule_end` datetime DEFAULT NULL,
  `create_user` varchar(128) DEFAULT NULL,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `group_code` varchar(128) DEFAULT '',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `task_code` varchar(128) NOT NULL DEFAULT '',
  `role_flag` int(1) NOT NULL DEFAULT '0',
  `data_offset` int(3) NOT NULL DEFAULT '-1',
  `valid_type` int(11) NOT NULL DEFAULT '0',
  `monitor_type` int(3) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=84 DEFAULT CHARSET=utf8;
+----+---------+-----------+-----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------+-----------+-----------+-------------+---------------------+-------+-------------+---------------------+---------------------+---------------------+-------------+---------------------+-----------------------------+---------------------+-----------+-----------+-------------+------------+--------------+
| id | flow_id | task_name | task_script                                                                                   | task_params                                                                  | task_type | call_type | call_quartz | data_time           | valid | last_status | last_call           | schedule_start      | schedule_end        | create_user | create_time         | group_code                  | update_time         | task_code | role_flag | data_offset | valid_type | monitor_type |
+----+---------+-----------+-----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------+-----------+-----------+-------------+---------------------+-------+-------------+---------------------+---------------------+---------------------+-------------+---------------------+-----------------------------+---------------------+-----------+-----------+-------------+------------+--------------+
| 83 |     959 | 流测试    | K:spark-submit --name #YARN_NAME# #CONF# --class #SJARCLAZZ# #SJARPATH# #REDISCONF# #TASKID#  | [{"ref":"20230904","code":"%conf_version%"},{"ref":"2","code":"%splitmax%"}] |         0 |        99 |             | 2023-09-12 18:07:11 |     2 |          21 | 2023-09-18 14:13:56 | 2023-09-12 18:07:11 | 2023-09-12 18:07:11 | 12          | 2023-09-12 18:07:11 | task_hive_import_group_test | 2023-09-18 14:13:56 |           |         9 |          -1 |          0 |            0 |
+----+---------+-----------+-----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------+-----------+-----------+-------------+---------------------+-------+-------------+---------------------+---------------------+---------------------+-------------+---------------------+-----------------------------+---------------------+-----------+-----------+-------------+------------+--------------+
----------------任务实例------------------------
CREATE TABLE `bd_task_current` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `task_id` bigint(20) NOT NULL,
  `task_batch` varchar(128) DEFAULT NULL,
  `task_script` varchar(1024) NOT NULL,
  `data_time` varchar(36) NOT NULL DEFAULT '',
  `valid` int(1) NOT NULL DEFAULT '0',
  `role_flag` int(2) NOT NULL DEFAULT '0',
  `schedule_start` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=390 DEFAULT CHARSET=utf8;
+-----+---------+----------------+---------------------------------------------------------------------------------------------------------+-----------+-------+-----------+---------------------+
| id  | task_id | task_batch     | task_script                                                                                             | data_time | valid | role_flag | schedule_start      |
+-----+---------+----------------+---------------------------------------------------------------------------------------------------------+-----------+-------+-----------+---------------------+
| 389 |      81 | 20230914182753 | S:spark-submit --name #YARN_NAME# #CONF# --class #JARCLAZZ# #JARPATH# #REDISCONF# #TASKID# 20230902 '*' | 20230902  |     2 |         9 | 2023-09-14 18:27:53 |
+-----+---------+----------------+---------------------------------------------------------------------------------------------------------+-----------+-------+-----------+---------------------+
--------------处理结果---------------------
 CREATE TABLE `bd_task_history` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `task_id` bigint(20) NOT NULL,
  `task_batch` varchar(128) NOT NULL,
  `task_name` varchar(128) NOT NULL,
  `task_script` varchar(1024) NOT NULL,
  `task_status` int(2) NOT NULL DEFAULT '0',
  `task_call` datetime DEFAULT NULL,
  `task_finish` datetime DEFAULT NULL,
  `cost` int(11) DEFAULT NULL,
  `flow_version` int(6) DEFAULT '0',
  `task_user` varchar(128) DEFAULT NULL,
  `data_time` varchar(36) NOT NULL DEFAULT '',
  `log` varchar(8192) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3940147 DEFAULT CHARSET=utf8;
+---------+---------+----------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+---------------------+---------------------+------+--------------+-----------+-----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 3940146 |      80 | 20230921172040 | 欧美日志     | S:spark-submit --name hive_load_event_huntress_om@@a72170117b0045da86e2db0d1f782f05@@80_20230921172040_0 --conf spark.files.ignoreMissingFiles=true --master yarn  --class com.sincetimes.bigdata.server.SparkProcessDriver /data/bigdata/flow/gbsp-processing-assembly-1.0.jar 10.104.13.152@@6379@@bigdata hive_load_event_huntress_om@@a72170117b0045da86e2db0d1f782f05@@80_20230921172040_0 20230920 '*'  |           1 | 2023-09-21 17:20:40 | 2023-09-21 17:27:00 |    0 |            5 | NULL      | 20230920  | task run success:hive_load_event_huntress_om@@a72170117b0045da86e2db0d1f782f05@@80_20230921172040_0
yarn url:http://gzcloud20:8088/cluster/app/application_1689749217869_0566
 |
+---------+---------+----------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+---------------------+---------------------+------+--------------+-----------+-----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+