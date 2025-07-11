#!/usr/bin/bash

# =============================================================================
# 功能: 
# 将本机日志压缩,然后上传到其他服务器
# 
# =============================================================================


# -----------------------------------------------------------------------------
# 功能: 读取配置文件(ini)
# 用法: __readINI [配置文件路径+名称] [节点名] [键值]
# 示例:	__readINI /config/warship.ini common name   结果赋值给 _readIni
#		name=$( __readINI warship.ini common name ) 将echo的结果赋值给变量name
#
# 原理:
# 1. -F '=' 表示用“=”作为分割符（多余的空格同样会被忽略;
# 2. /\['$SECTION'\]/ 和 /'${ITEM}'/ 是正则表达式，用于匹配节名和键名，注意在awk正则表达式中参数要用单引号括起来,\]表示转移大括号,大括号在正则中是关键字
# 3. /\['$SECTION'\]/{a=1} 表示逐行搜索到目标节时用变量a标记下来；
# 4. a==1&&$1~/'${ITEM}'/{print $2;exit} 表示当目标节已找到（a==1）并且当前行第一个参数匹配键名时，打印第二个参数（键值）并退出，这里exit是为了防止后面其它节有相同的键名。
#    	$1~/'${ITEM}'/ 表示变量1与正则表达式'${ITEM}'匹配   不匹配写法 !~
#		&&$1!~/'^#'/ 表示$1部分,不是以#号开头 !~
# -----------------------------------------------------------------------------
function __readINI() {
    INIFILE=$1; SECTION=$2; ITEM=$3
    _readIni=`awk -F '=' '/\['${SECTION}'\]/{a=1}a==1&&$1~/'${ITEM}'/&&$1!~/'^#'/{print $2;exit}' ${INIFILE}`
    echo ${_readIni}
}

# -----------------------------------------------------------------------------
# 功能: 截取字符串
# 用法:  __splitStr [字符串] [分隔符，一个字符] [返回第几个字符串]
# 示例: __splitStr abc,ab,c , 2        打印: ab
# -----------------------------------------------------------------------------
function __splitStr() {
    INSTR=$1; SPLIT=$2; NUM=$3
    echo "${INSTR}" | cut -d "${SPLIT}" -f ${NUM}
}

# -----------------------------------------------------------------------------
# 功能: 读取配置文件(ini),获取本地日志文件路径
# 用法:  __readINIDir [ini文件路径] common data_dir
# 示例: __readINIDir ${file} common data_dir  返回 /data/logupload
# -----------------------------------------------------------------------------
function __readINIDir() {
    DIR=$( __readINI $1 $2 $3 )

    _last=`echo ${DIR: -1}`

    if [ "${_last}" == "/" ]
    then
        echo ${DIR%?}
    else
        echo ${DIR}
    fi
}

# -----------------------------------------------------------------------------
# 功能: 拼接文件名
# 用法:  __saveFile [日期] [文件名] 
# 示例: __saveFile 2018-04-11 warship.log  day_20180411_warship.log
# -----------------------------------------------------------------------------
function __saveFile() {
    DATE=$1; NAME=$2
    _day=`echo ${DATE} | sed 's/-//g'`
    echo "day_${_day}_${NAME}"
}

# -----------------------------------------------------------------------------
# 功能: 获取年月
# 用法:  __getYearMonth [yyyy-mm-dd]
# 示例: __getYearMonth 2018-04-11  打印: 201804
# -----------------------------------------------------------------------------
function __getMonth() {
    DATE=$1
    _year=$( __splitStr ${DATE} "-" 1 )
    _month=$( __splitStr ${DATE} "-" 2 )
    echo "${_year}${_month}"
}
# 打印: 2018
function __getYear() {
    DATE=$1
    _year=$( __splitStr ${DATE} "-" 1 )
    echo "${_year}"
}

# -----------------------------------------------------------------------------
# 功能: 上传文件到cos
# 用法: __uploadCos [文件名,相对路径/绝对路径] [cos目录] [cos文件名] [文件大小]
# 示例: 将gamecenter.log.20180315.log文件上传到sincetimeslog2018/modership/目录下
#		__uploadCos gamecenter.log.20180315.log sincetimeslog2018/modernship/ gamecenter.log.20180315.log  100
#		返回结果: 上传成功返回 success
# -----------------------------------------------------------------------------
function __uploadCos() {
    LOGFILE=$1; DIR=$2; LOGNAME=$3; FILESIZE=$4

    ${coscmd_path}coscmd upload ${LOGFILE} ${DIR} > /dev/null 2>&1
    
    cosSize=`${coscmd_path}coscmd list ${DIR}${LOGNAME}  | awk '{print $2}'`
    
    if [ "${FILESIZE}" == "${cosSize}" ]
    then
        echo "success"
    else
        echo "failed"
    fi
}

# -----------------------------------------------------------------------------
# 功能: 自定义参数替换 ${logFileDate} 表示需要处理的日志的日期: xxxx-xx-xx
# 用法:  __paramReplace [需要替换的字符串] 
# 示例: __paramReplace ModernShipStat_{date}.log.tar.gz  输出 ModernShipStat_2018-04-26.log.tar.gz
# -----------------------------------------------------------------------------
function __paramReplace(){
	param=$1	
	param="${param/\{year\}/$( __getYear ${logFileDate} )}"	
	param="${param/\{month\}/$( __getMonth ${logFileDate} )}"	
	param="${param/\{date\}/${logFileDate}}"
	param="${param/\{short_data\}/${shortLogFileDate}}"
	echo $param
}

# -----------------------------------------------------------------------------
# 功能: 打印信息
# 用法:  __printInfo [信息] 
# 示例: __printInfo 参数不合法
# -----------------------------------------------------------------------------
function __printInfo(){
	#当前的时间 : 2018-04-27 10:13:04
	currentDate=$(date +'%Y-%m-%d %H:%M:%S')
	LOG=$1
	echo  "【${currentDate}】-【 info】:  ${LOG} "
}

function __printError(){
	#当前的时间 : 2018-04-27 10:13:04
	currentDate=`date +'%Y-%m-%d %H:%M:%S'`
	LOG=$1
	echo -e "\e[1;31m【${currentDate}】-【error】: ${LOG} \e[0m"
}


# -----------------------------------------------------------------------------
# 功能: 临时函数,打印参数
# -----------------------------------------------------------------------------
function __printParams(){
  echo -e "\n-----------------------参数-------------------------"
  echo "| logFileDate:${logFileDate}"
  echo "| shortLogFileDate:${shortLogFileDate}"
  echo "| source_log_file:${source_log_file}"
  echo "| tar_md5_file_dir:${tar_md5_file_dir}"
  echo "| tar_file_name:${tar_file_name}"
  echo "| md5_file_name:${md5_file_name}"
  echo "| md5CosDir:${md5CosDir}"
  echo "| ip:${ip}"
  echo "| username:${username}"
  echo "| password:${password}"
  echo "| destination_dir:${destination_dir}"
  echo -e "----------------------------------------------------\n"
}

# -----------------------------------------------------------------------------
# 功能: 判断路径是否存在
# 用法:  __isExist [文件路径] 
# 示例: __isExist /data/stat/Log_*/ModernShipStat_2018-01-31_*.log  返回0:存在,2:不存在
# -----------------------------------------------------------------------------
function __isExist(){
	FILEPATH=$1;
	ls ${FILEPATH} >/dev/null 2>&1 
	echo $?
}


# -----------------------------------------------------------------------------
# 功能:发送文件到其他机器
# -----------------------------------------------------------------------------
function __sendFile2OtherMachine(){
	FILENAME=$1
	lftp  -e "set sftp:connect-program 'ssh -oHostKeyAlgorithms=ssh-rsa,ssh-dss -oKexAlgorithms=diffie-hellman-group-exchange-sha1,diffie-hellman-group14-sha1'" -u ${username},${password} sftp://${ip} <<EOF
cd ${destination_dir}
put ${FILENAME}
by
EOF
	result=$(echo $?)
	if [ "${result}" != "0" ]
	then 
		echo "failed"
	else
		echo "success"
	fi
}


#---------------------------------------------------------------------------------------------
# 			1 判断处理
#---------------------------------------------------------------------------------------------

#1参数校验
if [ $# -lt 1 ]
then
        echo "[error1]:至少需要1个参数,[配置文件路径] [可选参数,skip_day,优先读取shell中的参数]"
		echo "[示例]:	./data_check_up2cos.sh warship.ini"
        exit
fi
configIniName=$1

#echo "configIniName:${configIniName}"

#2检查配置文件路径是否正确
if test -f "${configIniName}"
then
    configFile=${configIniName}
else
   echo "配置文件${configIniName}不存在,请检查路径"
   exit 
fi


#3读取配置文件
#向前跳过的天数
skipDay=$2
if [ ! -n "${skipDay}" ];then skipDay=$( __readINI ${configFile} common skip_day );fi
#原日志名
source_log_file=$( __readINIDir ${configFile} common source_log_file )
#tar文件,md5文件保存路径
tar_md5_file_dir=$( __readINIDir ${configFile} common tar_md5_file_dir )
#tar文件名
tar_file_name=$( __readINIDir ${configFile} common tar_file_name )
#md5文件名
md5_file_name=$( __readINIDir ${configFile} common md5_file_name )
#监控文件名
ip=$( __readINI ${configFile} destination ip)
username=$( __readINI ${configFile} destination username)
password=$( __readINI ${configFile} destination password)
destination_dir=$( __readINI ${configFile} destination destination_dir)

#4判断变量值是否存在
#变量不存在,则读取shell脚本的第二个参数
if [ ! -n "${source_log_file}" ];then echo  -e "\e[1;31m 文件${configIniName}下 common 变量 source_log_file 不存在 \e[0m";exit 1 ;fi
if [ ! -n "${tar_md5_file_dir}" ];then echo  -e "\e[1;31m 文件${configIniName}下 common 变量 tar_md5_file_dir 不存在 \e[0m";exit 1 ;fi
if [ ! -n "${tar_file_name}" ];then echo  -e "\e[1;31m 文件${configIniName}下 common 变量 tar_file_name 不存在 \e[0m";exit 1 ;fi
if [ ! -n "${md5_file_name}" ];then echo  -e "\e[1;31m 文件${configIniName}下 common 变量 md5_file_name 不存在 \e[0m";exit 1 ;fi
if [ ! -n "${ip}" ];then echo  -e "\e[1;31m 文件${configIniName}下 destination 变量 ip 不存在 \e[0m";exit 1 ;fi
if [ ! -n "${username}" ];then echo  -e "\e[1;31m 文件${configIniName}下 destination 变量 username 不存在 \e[0m";exit 1 ;fi
if [ ! -n "${password}" ];then echo  -e "\e[1;31m 文件${configIniName}下 destination 变量 password 不存在 \e[0m";exit 1 ;fi
if [ ! -n "${destination_dir}" ];then echo  -e "\e[1;31m 文件${configIniName}下 destination 变量 destination_dir 不存在 \e[0m";exit 1 ;fi


#5变量替换  {year}:2018,{month}:201804 {date}:2018-04-21
#获取当前日期:2018-04-27
today=`date +%F`
#获取向前推的某个日期:2018-04-26
logFileDate=`date -d "-${skipDay} day ${today}" +%Y-%m-%d`
shortLogFileDate=`date -d "-${skipDay} day ${today}" +%Y%m%d`

source_log_file=$( __paramReplace  ${source_log_file} )
tar_md5_file_dir=$( __paramReplace  ${tar_md5_file_dir} )
tar_file_name=$( __paramReplace  ${tar_file_name} )
md5_file_name=$( __paramReplace  ${md5_file_name} )

ip=$( __paramReplace  ${ip} )
username=$( __paramReplace  ${username} )
password=$( __paramReplace  ${password} )
destination_dir=$( __paramReplace  ${destination_dir} )



#---------------------------------------------------------------------------------------------
# 			2逻辑处理
#---------------------------------------------------------------------------------------------

#判断日志文件是否存在
#echo "${source_log_file}"
result=$(__isExist ${source_log_file})
#echo "$result"
__printParams
if [ "${result}" != "0" ]
   then
   	__printError  "${source_log_file} 文件不存在"
else
	#1判断tar,md5文件存放路径是否存在,不存在创建
	if [ ! -d "$tar_md5_file_dir" ]
	then
		mkdir -p $tar_md5_file_dir
	fi
   	
	#压缩文件
	if [ ! -e ${tar_md5_file_dir}/${tar_file_name} ]
	then 
		#echo "tar文件不存在"
		__printInfo "开始压缩 ${tar_md5_file_dir}/${tar_file_name}"
		tar -zcPf ${tar_md5_file_dir}/${tar_file_name} ${source_log_file}
	else	
		#echo "tar文件存在"
		__printError "待生成文件 ${tar_md5_file_dir}/${tar_file_name}     已存在,请检查(若要重新生成,请删除已存在文件)"	
	fi  
	
	
	if [ ! -e ${tar_md5_file_dir}/${tar_file_name} ]
	then
		#tar文件存在,md5文件不存在
		__printError  "${tar_md5_file_dir}${tar_file_name}     文件不存在,无法生成md5文件"
	
	elif [[  -e ${tar_md5_file_dir}/${tar_file_name} && ! -e ${tar_md5_file_dir}/${md5_file_name} ]]
	then 
		#echo "tar文件存在,md5文件不存在"
		#生成md5文件
		md5sum  ${tar_md5_file_dir}/${tar_file_name} >  ${tar_md5_file_dir}/${md5_file_name}
		
	elif [[  -e ${tar_md5_file_dir}/${tar_file_name} &&  -e ${tar_md5_file_dir}/${md5_file_name} ]]
	then
		__printError "待生成文件 ${tar_md5_file_dir}/${md5_file_name}     已存在,请检查(若要重新生成,请删除已存在文件)"	
	fi

	if [[  -e ${tar_md5_file_dir}/${tar_file_name} &&  -e ${tar_md5_file_dir}/${md5_file_name} ]]
	then
		__printInfo "生成 ${tar_file_name},${md5_file_name} success"
		
		#------ 2 上传文件到指定服务器 -------
		# __sendFile2OtherMachine 
		tarLogTag="failed"
		md5LogTag="failed"
		 #如果失败重试3次
		for i in $(seq 1 3)
		do
			#上传tar文件
			if [ "${tarLogTag}" == "failed" ]
			then
			   tarLogTag=$( __sendFile2OtherMachine ${tar_md5_file_dir}/${tar_file_name} )
			fi

			#上传md5文件
			if [ "${md5LogTag}" == "failed" ]
			then
			   md5LogTag=$( __sendFile2OtherMachine ${tar_md5_file_dir}/${md5_file_name} )
			fi
			
			if [[ "${tarLogTag}" == "failed" ]] || [[ "${md5LogTag}" == "failed" ]] 
			then
			   #处理可能是网络波动的因素,等待一段时间
			   let "j=i*5"
			   sleep $j
			fi
		done
		
		#打印结果
		if [ "${tarLogTag}" == "success" ]
		then
		   __printInfo "upload: [${tar_md5_file_dir}/${tar_file_name}] :${tarLogTag}"
		else
		  __printError "upload: [${tar_md5_file_dir}/${tar_file_name}] :${tarLogTag}"
		fi

		if [ "${md5LogTag}" == "success" ]
		then
		   __printInfo "upload: [${tar_md5_file_dir}/${md5_file_name}] :${md5LogTag}"
		else
		  __printError "upload: [${tar_md5_file_dir}/${md5_file_name}] :${md5LogTag}"
		fi

		# 删除临时压缩包
		#rm -rf ${tar_md5_file_dir}/${tar_file_name}
		#rm -rf ${tar_md5_file_dir}/${md5_file_name}
		__printInfo "===============================${logFileDate} finish==============================="
	fi
fi
