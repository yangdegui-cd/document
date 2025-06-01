#!/bin/bash
#龙女支付读库写入文件



if [[ $1 =~ ^[0-9]+$ ]]; then
  # If the input is a positive integer
  day=$(date -d "$1 years ago" "+%Y")
else
  # If the input is a date string
  day=$(date -d "$1" "+%Y")
fi

host=123.207.245.19
port=22
user=redwar
desdir=/data/redwar/redwar_pay_history
password=redwarupload

tempdir=/data/tmpdir/pay_history_list/
mkdir -p $tempdir

logname=paylist_${day}.data


mysql -h10.105.144.4 -uweb -pwardb\!@ -N  -e "select 'payment',warcommander.ordersqq.id,from_unixtime(warcommander.ordersqq.time/1000),warcommander.ordersqq.openid,warcommander.ordersqq.cmd,warcommander.ordersqq.amount,warcommander.ordersqq.isvip,warcommander.ordersqq.pid,warcommander.ordersqq.pamount,warcommander.ordersqq.billno,warcommander.ordersqq.resultNum,warcommander.ordersqq.platformNo,warcommander.ordersqq.region,warcommander.ordersqq.level,warcommander.ordersqq.billkey,warcommander.ordersqq.amt,warcommander.ordersqq.consuming,warcommander.ordersqq.uid,warcommander.ordersqq.payamt,warcommander.ordersqq.pubacctPayamt from warcommander.ordersqq where date_format(FROM_UNIXTIME(time/1000),'%Y')='$day'" -B -s | sed 's/\t/,/g' > ${tempdir}${logname}

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



