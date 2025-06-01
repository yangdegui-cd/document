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
user=longmu
desdir=/data/longmu/taiwan/payment
password=longmu@97$



tempdir=/data/tmpdir/pay_list/

server_logpath=/data/logs/sspaylog/
logname=sspay.log.${day}

mysql -ulongmu -pmulong -h10.200.18.15 -N  -e "select longmu.pay.* from longmu.pay where time>='${day} 00:00:00' and time<= '${day} 23:59:59'" -B -s | sed 's/\t/,/g' > ${tempdir}${logname}

echo 'pull success:'${logname}

#2.生成md5文件
md5sum ${tempdir}${logname} > ${tempdir}${logname}.md5
#3.上传文件
lftp -u ${user},${password} sftp://${host} <<EOF
cd ${desdir}
lcd ${tempdir}
put ${logname}
put ${logname}.md5
by
EOF
echo 'upload success:'${logname}
rm -f ${tempdir}${logname}
cat ${tempdir}${logname}.md5
rm -f ${tempdir}${logname}.md5
echo '========================='
