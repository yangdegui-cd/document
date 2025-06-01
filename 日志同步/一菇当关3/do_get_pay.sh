#!/bin/bash
#龙女支付读库写入文件



if [[ $1 =~ ^[0-9]+$ ]]; then
  # If the input is a positive integer
  day=$(date -d "$1 days ago" "+%Y-%m-%d")
else
  # If the input is a date string
  day=$(date -d "$1" "+%Y-%m-%d")
fi


host=119.28.83.11
port=22
user=mushroom
desdir=/data1/mushroom/tw/payment_app
password='mushroom@H3'



tempdir=/data/logs/paylist/
mkdir -p $tempdir
todata=paylist_${day}.data
echo 'start upload data:'${day}
#1.压缩
mysql -u root -p'dow3!20241012' -N  -e "select 'payment', dow3.t_pay.*  from dow3.t_pay where time>='${day} 00:00:00' and time<= '${day} 23:59:59'" -B -s | sed 's/\t/,/g' > ${tempdir}${todata}

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



cd /data/script
vim do_get_pay.sh
chmod +x do_get_pay.sh
./do_get_pay.sh 1
ll /data/logs/paylist/
