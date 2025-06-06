https://yakhuv4uq72.feishu.cn/docx/PcpRddDXRoIc0bxj20Bch5BYnlb 


### 列名: 钻石单价

	- Dow日本金币汇率: 0.56 (钻石/日元)
	- 龙女日本钻石汇率: 0.5  (钻石/日元)
	- 龙女欧美钻石汇率: 0.6  (钻石/美分)
	- 龙女韩国钻石汇率: 0.05 (钻石/韩元)
	- 龙女台湾钻石汇率: 2.0  (钻石/台币)


### 列名: 一次性道具收入(按月份汇总)
- 从一次性道具表中计算得出
- 计算公式: 一次性道具表中 同月份core_cost的总和 / 钻石单价

### 列名: 误差, 误差占比 (误差占比小于2%可忽略.)
- 从 月统计表中计算得出
- 计算公式: diff = core_cost - （corecost_once + corecost_forever + corecost_play）


# 修改配置

## 1. 修改各地区audit_daily脚本

	1. 修改库名(如果库名有变动)
	2. 修改表名(如果表名有变动)
	3. 其他修改

## 2. 让项目组提供最新的道具价格表

	一定要最新的! 最全的!  不然会出现数据不对
	
## 3. 将道具价格表导入到trino的配置表中
	- 先将道具信息表(包含 道具ID, 道具名, 道具类型(once, forever), 道具单价等信息)上传数据库.
	- dim_gserver_additem_itemid

## 4. 检查相关表是否数据完整
### 主要包括以下表
	1. r.dwd_gserver_additem_live       (道具消耗)
	2. r.dwd_gserver_costitem_live      (道具增加)
	3. r.dwd_gserver_diamondcost_live   (代币增加)
	4. w.dim_gserver_audit_item         (道具信息)
	5. w.dws_user_daily_di              (用户每日表)
	6. w.dws_user_info_di               (用户信息表) 

### 验证方式如下. 根据表名和日期调整.
```sql
 -- 数据完整xing
WITH date_sequence AS
         (SELECT SEQUENCE(DATE '2024-01-01', DATE '2024-06-30', INTERVAL '1' DAY) AS dates),
     date_table as
         (SELECT date FROM date_sequence CROSS JOIN UNNEST(dates) AS t(date))

SELECT ds.date
FROM date_table ds
WHERE NOT EXISTS (SELECT 1
                  FROM hive.huntress_om_w.dws_audit_daily_di dt
                  WHERE ds.date = dt.date)
ORDER BY ds.date;
```
## 5. 补全表数据
第三步中根据相关表查询结果数据去补全或核定数据.        
1. 如果是r库 联系后端同学补录或检查当天是否停服等原因导致的数据缺失
2. 如果是w库 自己去定时任务后台补全当天的数据.
3. dim_gserver_audit_item 道具信息表 一定要最全的. 不然会出现道具单价出错而导致的分摊不符合实际的情况

## 6. 确认代币与现实货币的汇率
找项目组确认. 有的项目需要 有的可能不需要. 根据分摊模型而定

## 7. 修改运行脚本的配置(dow_jp_conf)

```json
{
	"user": "taskdeploy",
	"password":"G5dAToqEzv7QK1UM",
	"out_path": "/Users/yangdegui/document/20250106-财审/dow_jp",
	"project_name": "Dow日本",
	"schema_prefix": "dow_jpnew",
	"begin_month": "2024-07",
	"end_month": "2024-12",
	"gold_rate": 0.56
}
```
---

# 计算数据

### 将修改后的audit_daily脚本设置到各地区的定时.  然后跑对应时间的数据

## 1. 使用 get_agg_data.rb 脚本,将数据导出并打包

```bash
CONF=./dow_jp_conf.json ruby get_agg_data.rb
```

## 2. 使用脚本导入到Mysql
rails c

```ruby
a = AuditService.new("龙女日本(新)")
a.sync_data("2024-07", "2024-12")
```



> 1和2选其中一个就可以. 根据需求来

