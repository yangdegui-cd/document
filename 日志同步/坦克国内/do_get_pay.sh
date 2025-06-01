#!/bin/bash
#龙女支付读库写入文件
# 10 0 * * * sh /data/script/do_get_pay.sh 1 >> /data/script/upload_paylist.log 2>&1 &


if [[ $1 =~ ^[0-9]+$ ]]; then
  # If the input is a positive integer
  day=$(date -d "$1 days ago" "+%Y-%m-%d")
else
  # If the input is a date string
  day=$(date -d "$1" "+%Y-%m-%d")
fi

host=123.207.245.19
port=22
user=tank
desdir=/data/logupload/tank_lianyun/payment
password=tank_KNAT_666666


tempdir=/data/tmpdir/pay_list/
todata=paylist_${day}.data
echo 'start upload data:'${day}
#1.压缩
mysql -h10.104.28.180 -utank -ptank@knat -N  -e "select 'payment', tank.pay.*  from tank.pay where time>='${day} 00:00:00' and time<= '${day} 23:59:59' and status = 0" -B -s | sed 's/\t/,/g' > ${tempdir}${todata}

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