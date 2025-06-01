require 'trino-client'
require 'json'
require 'csv'
require 'date'

def get_aggregation(client, schema_prefix, begin_month, end_month, project_name, out_path, agg_type, gold_rate = 1)
  begin_date = begin_month + "-01"
  end_date = Date.new(end_month.split("-")[0].to_i, end_month.split("-")[1].to_i, -1).strftime('%Y-%m-%d')
  sql = case agg_type
        when :"月统计"
          <<~SQL
            with user_daily as
            (select date, date_trunc('month', date) as month,
            role_id, channel,
            money, core_gain_addmoney as core_gain, core_cost_addmoney as core_cost,
            reduce(itemadd_daily_array, 0, (s, x) -> 
            s + coalesce((case when json_extract_scalar(x, '$.item_type') = 'once' then json_value(x, 'strict $.core_cost' returning double) else null end), 0), 
            s -> s) as corecost_once,
            reduce(itemadd_daily_array, 0, (s, x) -> 
            s + coalesce((case when json_extract_scalar(x, '$.item_type') = 'forever' then json_value(x, 'strict $.core_cost' returning double) else null end), 0), 
            s -> s) as corecost_forever,
            corecost_play,
            itemadd_daily_array
            from hive.#{schema_prefix}_w.dws_audit_daily_di
            where part_date >= '#{begin_date}' and part_date <= '#{end_date}'
            )

            select month, channel, 
            sum(money) as money,
            sum(core_gain) as core_gain,
            sum(core_cost) as core_cost,
            sum(corecost_once) as corecost_once,
            sum(corecost_forever) as corecost_forever,
            sum(corecost_play) as corecost_play
            from user_daily
            group by 1, 2
            order by month desc,channel
          SQL
        when :"一次性道具"
          <<~SQL
            with user_daily as
            (select date, date_trunc('month', date) as month,
            role_id, channel,
            filter(itemadd_daily_array, x -> json_extract_scalar(x, '$.item_type') = 'once') as itemadd_once,
            filter(itemcost_daily_array, x -> json_extract_scalar(x, '$.item_type') = 'once') as itemcost_once
            from hive.#{schema_prefix}_w.dws_audit_daily_di
            where part_date >= '#{begin_date}' and part_date <= '#{end_date}'
            ),
            
            user_itemadd_daily_unnest as
            (select date, month,
            role_id, channel,
            itemadd_once, itemcost_once,
            json_extract_scalar(items.item_info, '$.item_id') as item_id,
            json_extract_scalar(items.item_info, '$.item_name') as item_name,
            json_value(items.item_info, 'strict $.item_num' returning bigint) as item_num,
            json_value(items.item_info, 'strict $.core_cost' returning double) as core_cost
            from user_daily a
            cross join unnest(itemadd_once) as items(item_info)
            ),
            
            user_itemadd_daily as
            (select month, channel, item_id, item_name, sum(item_num) as item_add, sum(core_cost) as core_cost
            from user_itemadd_daily_unnest
            group by 1, 2, 3, 4
            ),
            
            user_itemcost_daily_unnest as
            (select date, month,
            role_id, channel,
            itemcost_once, itemcost_once,
            json_extract_scalar(items.item_info, '$.item_id') as item_id,
            json_value(items.item_info, 'strict $.item_num' returning bigint) as item_num
            from user_daily a
            cross join unnest(itemcost_once) as items(item_info)
            ),
            
            user_itemcost_daily as
            (select month, channel, item_id, sum(item_num) as item_cost
            from user_itemcost_daily_unnest
            group by 1, 2, 3
            )
            
            select a.*, b.item_cost, coalesce(c.#{schema_prefix === 'dow_jp' ? 'gold_value' : 'item_price'}, 0) / #{gold_rate || 1} as item_price
            from user_itemadd_daily a
            left join user_itemcost_daily b
            on a.month = b.month 
            and a.channel = b.channel
            and a.item_id = b.item_id
            left join hive.#{schema_prefix}_w.#{schema_prefix === 'dow_jp' ? 'dim_gserver_additem_itemid' : 'dim_gserver_audit_item'} c
            on a.item_id = c.item_id
            order by month desc,channel
          SQL
        when :"永久道具"
          <<~SQL
            with user_daily as
            (select date, date_trunc('month', date) as month,
            role_id, channel,
            filter(itemadd_daily_array, x -> json_extract_scalar(x, '$.item_type') = 'forever') as itemadd_forever
            from hive.#{schema_prefix}_w.dws_audit_daily_di
            where  part_date >= '#{begin_date}' and part_date <= '#{end_date}'
            ),
            
            user_itemadd_daily_unnest as
            (select date, month,
            role_id, channel,
            itemadd_forever,
            json_extract_scalar(items.item_info, '$.item_id') as item_id,
            json_extract_scalar(items.item_info, '$.item_name') as item_name,
            json_value(items.item_info, 'strict $.item_num' returning bigint) as item_num,
            json_value(items.item_info, 'strict $.core_cost' returning double) as core_cost
            from user_daily a
            cross join unnest(itemadd_forever) as items(item_info)
            ),
            
            user_itemadd_daily as
            (select month, channel, item_id, item_name, sum(item_num) as item_add, sum(core_cost) as core_cost
            from user_itemadd_daily_unnest
            group by 1, 2, 3, 4
            )
            
            select *
            from user_itemadd_daily
            order by month desc,channel 
          SQL
        when :"玩法消耗"
          <<~SQL
            with user_daily as
            (select date, date_trunc('month', date) as month,
            role_id, channel,
            corecost_play_array
            from hive.#{schema_prefix}_w.dws_audit_daily_di
            where  part_date >= '#{begin_date}' and part_date <= '#{end_date}'
            ),

            user_play_daily_unnest as
            (select date, month,
            role_id, channel,
            corecost_play_array,
            json_extract_scalar(plays.play_info, '$.reason') as reason,
            json_value(plays.play_info, 'strict $.corecost_play' returning double) as core_cost
            from user_daily a
            cross join unnest(corecost_play_array) as plays(play_info)
            ),

            user_play_daily as
            (select month, channel, reason, sum(core_cost) as core_cost
            from user_play_daily_unnest
            group by 1, 2, 3
            )

            select *
            from user_play_daily
            order by month desc,channel
          SQL
        end

  out_file = "#{out_path}/财审_#{agg_type}_#{project_name}_#{begin_date.gsub('-', '')}_#{end_date.gsub('-', '')}.csv"
  puts "out_path:\t#{out_file}"
  puts "sql:\t#{sql}"
  begin
    result = client.run(sql)

    # 确保结果不为空
    if result.any?
      # 将结果保存到 CSV 文件
      CSV.open(out_file, 'w') do |csv|
        csv << result[0].map(&:name) # 写入表头
        result[1].each do |row|
          csv << row
        end
      end
      puts "查询结果已保存到文件中: #{out_file}"
    else
      puts '查询结果为空'
    end
  rescue StandardError => e
    puts "查询出错: #{e}"
    _end_month = Date.parse(end_month + '-01')
    _begin_month = Date.parse(begin_month + '-01')
    if _end_month > _begin_month
      puts "尝试重新查询, 按月查询"
      while _begin_month <= _end_month
        _begin_month_str = _begin_month.strftime('%Y-%m')
        retry_count = 0
        begin
          get_aggregation(client, schema_prefix, _begin_month_str, _begin_month_str, project_name, out_path, agg_type)
        rescue StandardError => e
          puts "查询出错: #{e}"
          puts "重试次数: #{retry_count}"
          retry if (retry_count += 1) <= 3
        end
        _begin_month = _begin_month.next_month
      end
    end
  end
end

def get_client(user, password)
  Trino::Client.new(
    { server: 'trino.sincetimes.com:8443', # required option
      ssl: { verify: true },
      catalog: "hive",
      user: user,
      password: password,
      time_zone: "Asia/Shanghai",
      language: "Chinese",
      query_timeout: 60 * 60 * 2,
      http_debug: false
    }
  )
end

def get_config
  conf_path = ENV['CONF']
  if conf_path.nil?
    puts "env conf is required"
    exit 1
  end
  begin
    file = File.read(conf_path)
    return JSON.parse(file)
  rescue => e
    puts e
    exit 1
  end
end

puts "开始查询"
config = get_config
user = config["user"]
password = config["password"]
puts config
client = get_client(user, password)
puts "获取client成功"
schema_prefix = config["schema_prefix"]
out_path = config["out_path"]
project_name = config["project_name"]
begin_month = config["begin_month"] || "2024-01"
end_month = config["end_month"] || "2024-12"
gold_rate = config["gold_rate"] || 1
puts "删除原有文件"
rm_command = "rm -f #{out_path}/财审_#{project_name}.zip"
puts rm_command
system(rm_command)
rm_command = "rm -f #{out_path}/*.csv"
puts rm_command
system(rm_command)
[:"月统计", :"一次性道具", :"永久道具", :"玩法消耗"].each do |agg_type|
  get_aggregation(client, schema_prefix, begin_month, end_month, project_name, out_path, agg_type, gold_rate)
end
output_tar_file = "#{out_path}/财审_#{project_name}.zip"
tar_command = "zip -j #{output_tar_file} #{out_path}/*.csv"

puts "打包文件"
puts tar_command
system(tar_command)

if $?.exitstatus == 0
  puts "文件压缩成功: #{output_tar_file}"
else
  puts "文件压缩失败, 请手动压缩"
end
