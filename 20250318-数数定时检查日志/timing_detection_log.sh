#!/bin/bash

# 日志文件夹路径
TIME=$(date +%Y%m%d_%H%M)
LOG_DIR="/data1/shushu/mushroom2s_log2"
OUTPUT_DIR="/data1/shushu/mushroom2s_log"
OUTPUT_FILE="$OUTPUT_DIR/logs_$(date +%Y%m%d_%H%M).txt"


# 获取当前时间的整点时间戳（去掉秒数）
CURRENT_TIMESTAMP=$(date +%s)
CURRENT_TIMESTAMP=$((CURRENT_TIMESTAMP - (CURRENT_TIMESTAMP % 60)))

find "$LOG_DIR" -type f -name "*.csv" | while read -r file; do
    while IFS= read -r line; do
        # 提取日志行的时间列（第一列）
        LOG_TIME=$(echo "$line" | cut -d',' -f1)
        
    
        LOG_TIMESTAMP=$(date -d "$LOG_TIME" +%s 2>/dev/null)
        
        # 检查时间戳是否有效，并且是否在最近 5 分钟内
        if [[ -n "$LOG_TIMESTAMP" && $((CURRENT_TIMESTAMP - LOG_TIMESTAMP)) -le 300 ]]; then
            # 将符合条件的日志行写入输出文件
            echo "$line" >> "$OUTPUT_FILE"
        fi
    done < "$file"
done

echo "$TIME logs to $OUTPUT_FILE success"