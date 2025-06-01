
if [[ $1 =~ ^[0-9]+$ ]]; then
  # If the input is a positive integer
  day=$(date -d "$1 days ago" "+%Y-%m-%d")
else
  # If the input is a date string
  day=$(date -d "$1" "+%Y-%m-%d")
fi


host=119.28.41.240
port=22
desdir=/data1/warship/jp/payment
user='warship'
password='warship@240'

logdir=/data/stat/Log*/

tempdir=/data/tmpdir/pay_list/
todata=pay_list_jp_${day}.data
echo 'start upload data:'${day}
#1.压缩
mysql -h10.0.18.20 -uworldship -pworldship -Dworldship -N  -e "select 'payment',billno,openid,region,uid,channel,device,account,money,REPLACE(orderid, ',', ''),level,viplevel,status,time,payway,origin from pay where time>='${day} 00:00:00' and time<= '${day} 23:59:59' and status = 0" -B -s | sed 's/\t/,/g' > ${tempdir}${todata}

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