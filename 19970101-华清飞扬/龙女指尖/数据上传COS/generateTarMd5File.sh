#!/bin/bash
#=============================================================================
#  1目的:
#	根据原日志文件生成tar文件和md5文件
#  2脚本执行 
#	./generateTarMd5File.sh 1 表示如果当前日期是2018-04-27日,则处理2018-04-26日的日志文件
#  3定时任务执行(每天凌晨1:30执行脚本)
#	30 1 * * * /data/script/logupload/generateTarMd5File.sh 1 >> /data/script/logupload/generateTarMd5File.log 2>&1 &
#  4结果展示  
#	/data/stat/Log_*/ModernShipStat_2018-04-17_*.log 
#			==> /data/logupload/201804/ModernShipStat_2018-04-17.log.tar.gz
#		        ==> /data/logupload/201804/ModernShipStat_2018-04-17.log.tar.gz.md5
#=============================================================================

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
# 功能: 自定义参数替换
# 用法:  __paramReplace [需要替换的字符串] 
# 示例: __paramReplace ModernShipStat_{date}.log.tar.gz  输出 ModernShipStat_2018-04-26.log.tar.gz
# -----------------------------------------------------------------------------
function __paramReplace(){
	param=$1	
	param="${param/\{year\}/$( __getYear ${logFileDate} )}"	
	param="${param/\{month\}/$( __getMonth ${logFileDate} )}"	
	param="${param/\{date\}/${logFileDate}}"		
	echo $param
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
	echo $param
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
# 功能: 截取字符串
# 用法:  __splitStr [字符串] [分隔符，一个字符] [返回第几个字符串]
# 示例: __splitStr abc,ab,c , 2        打印: ab
# -----------------------------------------------------------------------------
function __splitStr() {
    INSTR=$1; SPLIT=$2; NUM=$3
    echo "${INSTR}" | cut -d "${SPLIT}" -f ${NUM}
}


# -----------------------------------------------------------------------------
# 功能: 临时函数,打印参数
# -----------------------------------------------------------------------------
function __printParams(){
	echo -e "\n-----------------------参数-------------------------"
	echo "| skipDay:${skipDay}"
	echo "| sourceLogFile:${sourceLogFile}"
	echo "| tarMd5FileDir:${tarMd5FileDir}"
	echo "| tarFileName:${tarFileName}"
	echo "| md5FileName:${md5FileName}"
	echo -e "----------------------------------------------------\n"
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



#---------------------------------------------------------------------------------------------
# 			1参数处理
#---------------------------------------------------------------------------------------------
#1参数校验
if [ $# -lt 1 ]
then
        echo "[error1]:至少需要1个参数,[配置文件路径] [可选参数,skip_day,优先读取shell中的参数]"
		echo "[示例]:	./generateTarMd5File.sh config.ini"
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
#如果shell参数不存在,则读取配置文件的参数
if [ ! -n "${skipDay}" ];then skipDay=$( __readINI ${configFile} common skip_day );fi

#日志原文件路径
sourceLogFile=$( __readINIDir ${configFile} common source_log_file )

#tar文件,md5文件保存路径
tarMd5FileDir=$( __readINIDir ${configFile} common tar_md5_file_dir )

#tar文件名
tarFileName=$( __readINIDir ${configFile} common tar_file_name )

#md5文件名
md5FileName=$( __readINIDir ${configFile} common md5_file_name )



#4判断变量值是否存在
#变量不存在,则读取shell脚本的第二个参数
if [ ! -n "${skipDay}" ];then echo  -e "\e[1;31m 变量skipDay不存在,请至少配置一个,优先读取shell参数 \e[0m";exit 1 ;fi
if [ ! -n "${sourceLogFile}" ];then echo  -e "\e[1;31m 文件${configIniName}下common变量source_log_file不存在 \e[0m";exit 1 ;fi
if [ ! -n "${tarMd5FileDir}" ];then echo  -e "\e[1;31m 文件${configIniName}下common变量tar_md5_file_dir不存在 \e[0m";exit 1 ;fi
if [ ! -n "${tarFileName}" ];then echo  -e "\e[1;31m 文件${configIniName}下common变量tar_file_name不存在 \e[0m";exit 1 ;fi
if [ ! -n "${md5FileName}" ];then echo  -e "\e[1;31m 文件${configIniName}下common变量md5_file_name不存在 \e[0m";exit 1 ;fi



#5变量替换  {year}:2018,{month}:201804 {date}:2018-04-21
#获取当前日期:2018-04-27
today=`date +%F`
#当前的时间 : 2018-04-27 10:13:04
currentDate=`date +'%Y-%m-%d %H:%M:%S'`
#获取向前推的某个日期(需要处理的日志日期):2018-04-26
logFileDate=`date -d "-${skipDay} day ${today}" +%Y-%m-%d`

#日志原文件路径  /data/stat/Log_*/ModernShipStat_2018-05-25_*.log
sourceLogFile=$( __paramReplace  ${sourceLogFile} )
#tar文件md5文件存放目录: /data/logupload/201804
tarMd5FileDir=$( __paramReplace  ${tarMd5FileDir} )
#tar文件名字 : ModernShipStat_2018-01-30.log.tar.gz
tarFileName=$( __paramReplace  ${tarFileName} )
#md5文件名字 : ModernShipStat_2018-01-30.log.tar.gz.md5
md5FileName=$( __paramReplace  ${md5FileName} )


#打印参数
#__printParams



#---------------------------------------------------------------------------------------------
# 			2逻辑处理
#---------------------------------------------------------------------------------------------


#判断日志文件是否存在
#echo "${sourceLogFile}"
result=$(__isExist ${sourceLogFile})
#echo "$result"

if [ "${result}" != "0" ]
   then
   	__printError  "${sourceLogFile} 文件不存在"
else
	#1判断tar,md5文件存放路径是否存在,不存在创建
	if [ ! -d "$tarMd5FileDir" ]
	then
		mkdir -p $tarMd5FileDir
	fi
   	
	#压缩文件
	if [ ! -e ${tarMd5FileDir}/${tarFileName} ]
	then 
		#echo "tar文件不存在"
		tar -zcPf ${tarMd5FileDir}/${tarFileName} ${sourceLogFile}
	else	
		#echo "tar文件存在"
		__printError "待生成文件 ${tarMd5FileDir}/${tarFileName}     已存在,请检查(若要重新生成,请删除已存在文件)"	
	fi  
	
	
	if [ ! -e ${tarMd5FileDir}/${tarFileName} ]
	then
		#tar文件存在,md5文件不存在
		__printError  "${tarMd5FileDir}${tarFileName}     文件不存在,无法生成md5文件"
	
	elif [[  -e ${tarMd5FileDir}/${tarFileName} && ! -e ${tarMd5FileDir}/${md5FileName} ]]
	then 
		#echo "tar文件存在,md5文件不存在"
		#生成md5文件
		md5sum  ${tarMd5FileDir}/${tarFileName} >  ${tarMd5FileDir}/${md5FileName}
		
	elif [[  -e ${tarMd5FileDir}/${tarFileName} &&  -e ${tarMd5FileDir}/${md5FileName} ]]
	then
		__printError "待生成文件 ${tarMd5FileDir}/${md5FileName}     已存在,请检查(若要重新生成,请删除已存在文件)"	
	fi

	if [[  -e ${tarMd5FileDir}/${tarFileName} &&  -e ${tarMd5FileDir}/${md5FileName} ]]
	then
		__printInfo "${sourceLogFile} 处理完成(生成tar文件,和md5文件)"
	fi
fi