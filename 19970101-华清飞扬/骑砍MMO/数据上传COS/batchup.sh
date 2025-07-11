#! /bin/bash
#----------------------------------------
# 批量上传脚本
# 使用方式:./batchup.sh 20180528 20180601
#----------------------------------------

#1参数校验
if [ $# -lt 2 ]
then
	echo "[error1]:至少需要2个参数,[开始时间] [结束时间]"
	echo "[示例]:	./batchup.sh 20180528 20180601"
	exit
fi
stime=$1
etime=$2

##将输入的日期转为的时间戳格式
startDate=`date -d "${stime}" +%s`
endDate=`date -d "${etime}" +%s`
nowData=`date -d "$(date +%F)" +%s`
##计算两个时间戳的差值除于每天86400s即为天数差
endDay=`expr $(expr ${nowData} - ${endDate}) / 86400`
startDay=`expr $(expr ${nowData} - ${startDate}) / 86400`

#echo $endDate==$endDay
#echo $startDate==$startDay


#批量上传
for((i=$endDay;i<=$startDay;i++))
do
	/data/script/cos_zhaohe_japan_ad/uplog2cos.sh /data/script/cos_zhaohe_japan_ad/config.ini $i >> /data/script/cos_zhaohe_japan_ad/uplog2cos.log 2>&1
done
