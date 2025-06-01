#!/bin/bash
#龙女支付读库写入文件



if [[ $1 =~ ^[0-9]+$ ]]; then
  # If the input is a positive integer
  day=$(date -d "$1 days ago" "+%Y-%m-%d")
else
  # If the input is a date string
  day=$(date -d "$1" "+%Y-%m-%d")
fi

host=119.28.41.240
port=22
user=zhanguo
desdir=/data2/zhanguo/jp/payment
password=zhanguo@240


tempdir=/data/tmpdir/pay_list/
mkdir -p ${tempdir}
todata=paylist_${day}.data
echo 'start upload data:'${day}
#1.压缩
mysql  -hdow-mysql.cdsxxlgd4fbn.ap-northeast-1.rds.amazonaws.com -ustatreadonly -pdowHQjp0415 -Ddow  -N  -e "select *  from t_pay where time>='${day} 00:00:00' and time<= '${day} 23:59:59'" -B -s | sed 's/\t/,/g' > ${tempdir}${todata}

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