#!/bin/bash
# =============================================================================
# 功能: 
# 上传日志到cos如下目录下
# sincetimeslog2018/modernship/201801/LogStat_2018-01-01.log.tar.gz  
# sincetimeslog2018/modernship/md5/LogStat_2018-01-01.log.tar.gz.md5
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
  echo "| skipDay:${skipDay}"
  echo "| tarMd5FileDir:${tarMd5FileDir}"
  echo "| tarFileName:${tarFileName}"
  echo "| md5FileName:${md5FileName}"
  echo "| tarCosDir:${tarCosDir}"
  echo "| md5CosDir:${md5CosDir}"
  echo "| coscmd_path:${coscmd_path}"
  echo "| ftp_monitor_dir:${ftp_monitor_dir}"
  echo "| ftp_monitor_dir1:${ftp_monitor_dir1}"
  echo "| cos_monitor_dir:${cos_monitor_dir}"
  echo "| cos_monitor_dir1:${cos_monitor_dir1}"
  echo "| monitor_file:${monitor_file}"
  echo -e "----------------------------------------------------\n"
}


# -----------------------------------------------------------------------------
# 功能: 写入内容到文件
# -----------------------------------------------------------------------------
function __wirteToFtpFile(){
  	content=$1	
	if [ ! -d "$ftp_monitor_dir" ]
	then
		# 路径不存在创建路径
		mkdir -p $ftp_monitor_dir
		chmod 777 $ftp_monitor_dir
	fi
	if [ ! -d "$ftp_monitor_dir1" ]
	then
		# 路径不存在创建路径
		mkdir -p $ftp_monitor_dir1
		chmod 777 $ftp_monitor_dir1
	fi
	echo ${content} > ${ftp_monitor_dir}/${monitor_file}
	echo ${content} > ${ftp_monitor_dir1}/${monitor_file}
}

function __wirteToCosFile(){
	content=$1	
	if [ ! -d "$cos_monitor_dir" ]
	then
		# 路径不存在创建路径
		mkdir -p $cos_monitor_dir
		chmod 777 $cos_monitor_dir		
	fi
	if [ ! -d "$cos_monitor_dir1" ]
	then
		# 路径不存在创建路径
		mkdir -p $cos_monitor_dir1
		chmod 777 $cos_monitor_dir1		
	fi
	echo ${content} > ${cos_monitor_dir}/${monitor_file}
	echo ${content} > ${cos_monitor_dir1}/${monitor_file}
}

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

#tar文件,md5文件保存路径
tarMd5FileDir=$( __readINIDir ${configFile} common tar_md5_file_dir )

#tar文件名
tarFileName=$( __readINIDir ${configFile} common tar_file_name )

#md5文件名
md5FileName=$( __readINIDir ${configFile} common md5_file_name )

#tar文件在cos上储存路径 
tarCosDir=$( __readINI ${configFile} common tar_cos_dir)

#md5文件在cos保存的路径
md5CosDir=$( __readINI ${configFile} common md5_cos_dir)

#md5文件在cos保存的路径
coscmd_path=$( __readINI ${configFile} common coscmd_path)

#ftp监控路径
ftp_monitor_dir=$( __readINI ${configFile} monitor ftp_monitor_dir)
ftp_monitor_dir1=$( __readINI ${configFile} monitor ftp_monitor_dir1)

#cos监控路径
cos_monitor_dir=$( __readINI ${configFile} monitor cos_monitor_dir)
cos_monitor_dir1=$( __readINI ${configFile} monitor cos_monitor_dir1)

#监控文件名
monitor_file=$( __readINI ${configFile} monitor monitor_file)

#4判断变量值是否存在
#变量不存在,则读取shell脚本的第二个参数
if [ ! -n "${tarMd5FileDir}" ];then echo  -e "\e[1;31m 文件${configIniName}下common变量tar_md5_file_dir不存在 \e[0m";exit 1 ;fi
if [ ! -n "${tarFileName}" ];then echo  -e "\e[1;31m 文件${configIniName}下common变量tar_file_name不存在 \e[0m";exit 1 ;fi
if [ ! -n "${md5FileName}" ];then echo  -e "\e[1;31m 文件${configIniName}下common变量md5_file_name不存在 \e[0m";exit 1 ;fi
if [ ! -n "${tarCosDir}" ];then echo  -e "\e[1;31m 文件${configIniName}下common变量tar_cos_dir不存在 \e[0m";exit 1 ;fi
if [ ! -n "${md5CosDir}" ];then echo  -e "\e[1;31m 文件${configIniName}下common变量md5_cos_dir不存在 \e[0m";exit 1 ;fi
#日志监控的变量
if [ ! -n "${ftp_monitor_dir}" ];then echo  -e "\e[1;31m 文件${configIniName}下monitor变量ftp_monitor_dir不存在 \e[0m";exit 1 ;fi
if [ ! -n "${ftp_monitor_dir1}" ];then echo  -e "\e[1;31m 文件${configIniName}下monitor变量ftp_monitor_dir1不存在 \e[0m";exit 1 ;fi
if [ ! -n "${cos_monitor_dir}" ];then echo  -e "\e[1;31m 文件${configIniName}下monitor变量cos_monitor_dir不存在 \e[0m";exit 1 ;fi
if [ ! -n "${cos_monitor_dir1}" ];then echo  -e "\e[1;31m 文件${configIniName}下monitor变量cos_monitor_dir1不存在 \e[0m";exit 1 ;fi
if [ ! -n "${monitor_file}" ];then echo  -e "\e[1;31m 文件${configIniName}下monitor变量monitor_file不存在 \e[0m";exit 1 ;fi



#5变量替换  {year}:2018,{month}:201804 {date}:2018-04-21
#获取当前日期:2018-04-27
today=`date +%F`
#获取向前推的某个日期:2018-04-26
logFileDate=`date -d "-${skipDay} day ${today}" +%Y-%m-%d`
shortLogFileDate=`date -d "-${skipDay} day ${today}" +%Y%m%d`

tarMd5FileDir=$( __paramReplace  ${tarMd5FileDir} )
tarFileName=$( __paramReplace  ${tarFileName} )
md5FileName=$( __paramReplace  ${md5FileName} )
tarCosDir=$( __paramReplace  ${tarCosDir} )
md5CosDir=$( __paramReplace  ${md5CosDir} )
ftp_monitor_dir=$( __paramReplace  ${ftp_monitor_dir} )
ftp_monitor_dir1=$( __paramReplace  ${ftp_monitor_dir1} )
cos_monitor_dir=$( __paramReplace  ${cos_monitor_dir} )
cos_monitor_dir1=$( __paramReplace  ${cos_monitor_dir1} )
monitor_file=$( __paramReplace  ${monitor_file} )

#打印参数
#__printParams



#6校验tar文件和md5文件是否存在,md5值是否相同
logTag="log_not_found"
if test -f "${tarMd5FileDir}/${tarFileName}"
then
    if test -f "${tarMd5FileDir}/${md5FileName}"
    then
        oldMD5=`cat ${tarMd5FileDir}/${md5FileName} | awk '{print $1}'`;
        newMD5=`md5sum -b ${tarMd5FileDir}/${tarFileName} | awk '{print $1}'`
        if [ "${newMD5}" == "${oldMD5}" ]
        then
            logTag="success"
			__wirteToFtpFile success
	    else
	    	__printError ${tarMd5FileDir}/${tarFileName}与md5文件不匹配
			__wirteToFtpFile failed
        fi
    else
	    __printError ${tarMd5FileDir}/${md5FileName}文件不存在
		__wirteToFtpFile md5_not_found
    fi
else
	__printError ${tarMd5FileDir}/${tarFileName}文件不存在
	__wirteToFtpFile file_not_found
fi


#7上传文件
#是否上传成功的标记
if [ "${logTag}" == "success" ]
then
    tarCosLogTag="failed"
    md5CosLogTag="failed"
    tarFilesize=`ls -l ${tarMd5FileDir}/${tarFileName} | awk '{print $5}'`
    md5Filesize=`ls -l ${tarMd5FileDir}/${md5FileName} | awk '{print $5}'`
    #如果失败重试3次
    for i in $(seq 1 3)
    do
    	#上传tar文件
        if [ "${tarCosLogTag}" == "failed" ]
        then
           tarCosLogTag=$( __uploadCos ${tarMd5FileDir}/${tarFileName} ${tarCosDir} ${tarFileName} ${tarFilesize} )
        fi

		#上传md5文件
		if [ "${md5CosLogTag}" == "failed" ]
        then
           md5CosLogTag=$( __uploadCos ${tarMd5FileDir}/${md5FileName} ${md5CosDir} ${md5FileName} ${md5Filesize} )
        fi
		
		if [[ "${tarCosLogTag}" == "failed" ]] || [[ "${md5CosLogTag}" == "failed" ]] 
		then
		   #处理可能是网络波动的因素,等待一段时间
		   let "j=i*5"
		   sleep $j
		fi
    done
    
	#打印结果
	if [ "${tarCosLogTag}" == "success" ]
	then
	   __printInfo "cos upload: [${tarMd5FileDir}/${tarFileName}] :${tarCosLogTag}"
	else
	  __printError "cos upload: [${tarMd5FileDir}/${tarFileName}] :${tarCosLogTag}"
	fi

	if [ "${md5CosLogTag}" == "success" ]
	then
	   __printInfo "cos upload: [${tarMd5FileDir}/${md5FileName}] :${md5CosLogTag}"
	else
	  __printError "cos upload: [${tarMd5FileDir}/${md5FileName}] :${md5CosLogTag}"
	fi
	if [[ "${tarCosLogTag}" == "success" ]] && [[ "${md5CosLogTag}" == "success" ]] 
	then
		__wirteToCosFile success
	else
		__wirteToCosFile failed
	fi
else
	__wirteToCosFile error
fi
__printInfo "===============================${logFileDate} finish==============================="
__printParams
