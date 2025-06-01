day=$1

host=119.28.41.240
user=projects
password=az8ykkm1
desdir=/data2/projects/japan/payment

localdir=/data/bigdata/data/dpay/
file_name=pay_${day}.log

cat ${localdir}/pay_${day}_*.log > ${localdir}/${file_name}
lftp -u ${user},${password} sftp://${host} <<EOF
cd ${desdir}
lcd ${localdir}
put ${file_name}
by
EOF
echo 'upload success:'${file_name}
rm -f ${localdir}${file_name}
echo '========================='