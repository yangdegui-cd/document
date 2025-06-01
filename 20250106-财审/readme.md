https://yakhuv4uq72.feishu.cn/docx/PcpRddDXRoIc0bxj20Bch5BYnlb 


列名: 钻石单价

Dow日本金币汇率: 0.56 (钻石/日元)
龙女日本钻石汇率: 0.5  (钻石/日元)
龙女欧美钻石汇率: 0.6  (钻石/美分)
龙女韩国钻石汇率: 0.05 (钻石/韩元)
龙女台湾钻石汇率: 2.0  (钻石/台币)


列名: 一次性道具收入(按月份汇总)
从 一次性道具表中计算得出

计算公式: 一次性道具表中 同月份core_cost的总和 / 钻石单价

列名: 误差, 误差占比 (误差占比小于2%可忽略.)
从 月统计表中计算得出

计算公式: diff = core_cost - （corecost_once + corecost_forever + corecost_play）


# 修改配置

## 修改各地区audit_daily脚本
	1. 修改库名(如果库名有变动)
	2. 修改表名(如果表名有变动)
	3. 其他修改

## 让项目组提供最新的道具价格表
	
## 将道具价格表导入到trino的配置表中, dim_gserver_additem_itemid

## 修改运行脚本的配置(dow_jp_conf)

```
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

# 计算数据

## 将修改后的audit_daily脚本设置到各地区的定时.  然后跑对应时间的数据

## 1. 使用 get_agg_data.rb 脚本,将数据导出并打包
```
	CONF=./dow_jp_conf.json ruby get_agg_data.rb
```

## 2. 使用脚本导入到Mysql
rails c

```
	 a = AuditService.new("龙女日本(新)")
	 a.sync_data("2024-07", "2024-12")

```

> 1和2选其中一个就可以. 根据需求来

