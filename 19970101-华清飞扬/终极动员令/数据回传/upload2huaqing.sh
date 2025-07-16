#!/bin/bash
set -euo pipefail

# 参数验证
if [ $# -ne 1 ]; then
    echo "Usage: $0 <days_ago|date_string>"
    echo "Example: $0 1      # 1 day ago"
    echo "Example: $0 2023-08-01  # specific date"
    exit 1
fi

# 日志清理函数
cleanup() {
    [ -n "${merged_file:-}" ] && rm -f "$merged_file"
    [ -n "${TEM_PATH:-}" ] && rm -f "${TEM_PATH}${ZIP_NAME}" "${TEM_PATH}${ZIP_MD5_NAME}"
}
trap cleanup EXIT

# 日期处理
export TZ=Asia/Shanghai
if [[ $1 =~ ^[0-9]+$ ]]; then
    day=$(date -d "$1 days ago" "+%Y-%m-%d")
else
    if ! date -d "$1" >/dev/null 2>&1; then
        echo "Invalid date format: $1"
        exit 1
    fi
    day=$(date -d "$1" "+%Y-%m-%d")
fi

# 路径配置
shopt -s nullglob
LOG_PATHS=(/data/games/*/GameServer/logBus/)
LOG_NAME="report.log_${day}.log"
TEM_PATH="/data/logupload/zjdyl_tw/"
mkdir -p "$TEM_PATH"

# 文件名配置
MERGE_FILE_NAME="ZjdylTWStat_${day}.log"
ZIP_NAME="ZjdylTWStat_${day}.tar.gz"
ZIP_MD5_NAME="ZjdylTWStat_${day}.tar.gz.md5"
merged_file="${TEM_PATH}${MERGE_FILE_NAME}"

# 合并日志文件
found_files=0
temp_counter=$(mktemp)

for log_path in "${LOG_PATHS[@]}"; do
    if [ -d "$log_path" ]; then
        echo "Processing logs in: $log_path"
        find "$log_path" -name "$LOG_NAME" -print0 | while IFS= read -r -d '' file; do
            cat "$file" >> "$merged_file"
            echo 1 >> "$temp_counter"
        done
    fi
done

# 获取文件计数
if [ -f "$temp_counter" ]; then
    found_files=$(wc -l < "$temp_counter")
    rm -f "$temp_counter"
fi

if [ "$found_files" -eq 0 ]; then
    echo "No log files found for date $day in:"
    printf "  - %s\n" "${LOG_PATHS[@]}"
    exit 1
fi

# 压缩文件
if ! tar -czf "${TEM_PATH}${ZIP_NAME}" -C "$TEM_PATH" "${MERGE_FILE_NAME}"; then
    echo "Failed to create tar.gz archive"
    exit 1
fi

# MD5校验
if ! md5sum "${TEM_PATH}${ZIP_NAME}" | awk '{print $1}' > "${TEM_PATH}${ZIP_MD5_NAME}"; then
    echo "Failed to create MD5 checksum"
    exit 1
fi

# 传输配置
host=119.28.83.11
port=22
user=zjdyl
desdir="/data1/logupload/zjdyl_tw/"
password='zjdyl$2252'

# LFTP传输
for attempt in {1..3}; do
    if lftp -u "$user,$password" sftp://$host:$port <<EOF
set net:timeout 30
set net:max-retries 3
set sftp:auto-confirm yes
mkdir -p $desdir
put -O $desdir "${TEM_PATH}${ZIP_NAME}"
put -O $desdir "${TEM_PATH}${ZIP_MD5_NAME}"
bye
EOF
    then
        echo "${ZIP_NAME} and ${ZIP_MD5_NAME} transferred successfully to $desdir (attempt $attempt)"
        break
    else
        echo "Transfer attempt $attempt failed"
        [ $attempt -eq 3 ] && exit 1
        sleep 10
    fi
done

exit 0