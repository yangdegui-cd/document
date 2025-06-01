#!/bin/bash
#龙女支付读库写入文件



if [[ $1 =~ ^[0-9]+$ ]]; then
  # If the input is a positive integer
  day=$(date -d "$1 days ago" "+%Y-%m-%d")
else
  # If the input is a date string
  day=$(date -d "$1" "+%Y-%m-%d")
fi

host=119.28.84.97
port=22
desdir=/data/logupload/ship/germany/ship_germany_payment_app
user='logupload'
password='upload@hq'

logdir=/data/stat/Log*/

tempdir=/data/tmpdir/pay_list/
mkdir -p $tempdir
todata=gamepaylist_${day}.data
echo 'start upload data:'${day}
#1.压缩
mysql -h10.0.0.12 -uworldship -pworldship -Dworldship -N  -e "select * from pay where time>='${day} 00:00:00' and time<= '${day} 23:59:59'" -B -s | sed 's/\t/,/g' > ${tempdir}${todata}
echo 'tar success:'${todata}

#2.生成md5文件
md5sum ${tempdir}${todata} > ${tempdir}${todata}.md5
#3.上传文件
lftp -u ${user},${password} sftp://${host} <<EOF
cd ${desdir}
lcd ${tempdir}
put ${todata}
put ${todata}.md5
by
EOF
echo 'upload success:'${todata}
rm -f ${tempdir}${todata}
cat ${tempdir}${todata}.md5
rm -f ${tempdir}${todata}.md5
echo '========================='

