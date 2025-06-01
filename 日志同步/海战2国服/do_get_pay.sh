#!/bin/bash
#龙女支付读库写入文件



if [[ $1 =~ ^[0-9]+$ ]]; then
  # If the input is a positive integer
  day=$(date -d "$1 days ago" "+%Y-%m-%d")
else
  # If the input is a date string
  day=$(date -d "$1" "+%Y-%m-%d")
fi



tempdir=/data/logs/paylist/
mkdir -p $tempdir
todata=paylist_${day}.data
echo 'start upload data:'${day}
#1.压缩
mysql -h10.66.31.57 -uweb -pwarship123 -N  -e "select 'payment', warship.pay.*  from warship.pay where time>='${day} 00:00:00' and time<= '${day} 23:59:59'" -B -s | sed 's/\t/,/g' > ${tempdir}${todata}

echo 'tar success:'${todata}

#2.生成md5文件
md5sum ${tempdir}${todata} > ${tempdir}${todata}.md5
