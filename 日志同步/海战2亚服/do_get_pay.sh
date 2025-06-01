
#!/bin/bash
if [[ $1 =~ ^[0-9]+$ ]]; then
  # If the input is a positive integer
  day=$(date -d "$1 days ago" "+%Y-%m-%d")
else
  # If the input is a date string
  day=$(date -d "$1" "+%Y-%m-%d")
fi

host=119.28.41.240
port=22
desdir=/data1/warship2/asia/payment
user='warship2'
password='warship2@240'

tempdir=/data/logs/paylist/
mkdir -p $tempdir
todata=pay_app_jp_${day}.data
echo 'start upload data:'${day}
#1.压缩
mysql -h10.144.78.91 -ustat -p123456 -Dgame_warship -N  -e "select 'payment',billno,openid,itemid,region,uid,channel,device,'' as account,money,orderid,level,viplevel,status,time,payway,devicecode,origin,currency,'' as id,'' as paychannel, '' as amount, '' as gate from pay where time>='${day} 00:00:00' and time<= '${day} 23:59:59'" -B -s | sed 's/\t/,/g' > ${tempdir}${todata}

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