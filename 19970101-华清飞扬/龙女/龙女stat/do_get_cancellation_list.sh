#!/bin/bash
#!/bin/bash
# do_get_cancellation_paylist.sh


if [[ $1 =~ ^[0-9]+$ ]]; then
  # If the input is a positive integer
  day=$(date -d "$1 days ago" "+%Y-%m-%d")
else
  # If the input is a date string
  day=$(date -d "$1" "+%Y-%m-%d")
fi

#从库机器
host="10.10.10.11"
user="ec2-user"
pem_key="/home/ec2-user/.pubpem/hq_huntress_jpweb_login.pem"

#从库机器上的临时目录
server_tempdir="/tmp/cancellation_paylist/"
#本地保存临时目录
local_tempdir="/data/tmpdir/cancellation_paylist/"

temfile=cancellation_paylist_${day}.data

grep_command="grep cancellationNotice /data/logs/paylog/pay.log.$day|grep REFUND | awk -F'result:' '{print \$2}' > $server_tempdir$temfile"

ssh -i $pem_key $user@$host "$grep_command"
echo 'tar cancellation paylist success: '$temfile

scp -i $pem_key $user@$host:$server_tempdir$temfile $local_tempdir

echo 'pull cancellation paylist success: '$temfile
ssh -i $pem_key $user@$host "rm -f  $server_tempdir$temfile"
md5sum ${local_tempdir}${temfile} > ${local_tempdir}${temfile}.md5

国内服务器
host=119.28.84.97
port=22
user=huntresshq
desdir=/data/huntress/jp/payment_refund
password=aRVv!9ED


#3.上传文件
lftp -u ${user},${password} sftp://${host} <<EOF
cd ${desdir}
lcd ${local_tempdir}
put ${temfile}
put ${temfile}.md5
by
EOF

echo 'upload success:'${temfile}
rm -f ${local_tempdir}${temfile}
cat ${local_tempdir}${temfile}.md5
rm -f ${local_tempdir}${temfile}.md5
echo '========================='

