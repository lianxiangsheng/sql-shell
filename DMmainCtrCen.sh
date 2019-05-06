#!/bin/bash
#export NLS_LANG="SIMPLIFIED CHINESE_CHINA.ZHS16GBK"
#echo $NLS_LANG
cd /home/oracle/jobctrl/DM
#date +%Y%m%d" "%T" ---DMmainCtrCen.sh start---">>main.log
read user password server key < DMlocInst.cfg
USER=$user
PASSWORD=`/home/oracle/jobctrl/ENDE/des3_decrypt.sh $password $key`
SERVER=$server
#echo $USER $PASSWORD $SERVER
JOBPART=$1
source /home/oracle/jobctrl/DM/DMgetFunction.sh
getTime1=`date +%Y%m%d%H%M%S`
echo "TIME:"$getTime1
##判断是否启动
vcStart=`isStart $USER $PASSWORD $SERVER`
#echo "isstart"$vcStart
if [ "$vcStart"x = "0"x ] || [ -z "$vcStart" ] 
then 
  #echo "nstart"
  exit 0
fi

##获取作业信息
vcXmlinfo=`getJobdet $USER $PASSWORD $SERVER $JOBPART`
echo "vcXmlinfo"$vcXmlinfo
getTime2=`date +%Y%m%d%H%M%S`
getTime3=`expr $getTime2 - $getTime1`
##时间超长 退出
if [ $getTime3 -ge 60 ]
then 
exit 0
fi
if [ -z "$vcXmlinfo" ]
then 
  exit 0
fi
##解析作业信息
vcJobid=`getXmls "$vcXmlinfo" JOB_ID`
vcTotaljobdet=`getXmls "$vcXmlinfo" TOTALJOBDET`
vcUsrname=`getXmls "$vcXmlinfo" USRNAME`
vcUsrpwd=`getXmls "$vcXmlinfo" USRPWD`
vcTnsname=`getXmls  "$vcXmlinfo" TNSNAME`
viMaxrunjob=`getXmls "$vcXmlinfo" MAXRUNJOB`
viLastdate=`getXmls "$vcXmlinfo" LASTDATDATE`
##判断作业执行数是否达到最大设置 20180620优化 米辉翀
viSubjob=`ps -ef|grep DMperformPro.sh|grep -v "grep DMperformPro.sh"|awk -F " " '{if($NF=="'''$vcTnsname'''"){print $NF}}'|wc -l|awk '{print $1}'`
##echo "viSubjob:"$viSubjob
viSubjob=`expr $viSubroutine / 2`
if [ $viSubjob -ge $viMaxrunjob ]
then
  exit 0
fi

##开始作业 更新作业状态 记录作业信息
vcJobbgn=`jobBegin $vcJobid $USER $PASSWORD $SERVER`
vcJobbgnStat=`echo "$vcJobbgn"|awk -F " " '{print $1}'`
if [ $vcJobbgnStat == "1" ]
then
 echo "jobBegin return 1">>job_exit.log
 exit 0
fi
##循环子作业
i=1
while(($i<=$vcTotaljobdet))
do
  vcJobstr=`getXmls "$vcXmlinfo" JOBDET$i` 
  vcJobdetid=`getXmls "$vcJobstr" JOB_DET_ID`
  vcJobcmdtype=`getXmls "$vcJobstr" JOB_CMD_TYPE`
  vcJobcmd=`getXmls  "$vcJobstr" JOB_CMD`
  vcJoberrs=`getXmls "$vcJobstr" ERR_RETRY_TIME`      
  ##执行shell程序
  if [ $vcJobcmdtype -eq 1 ]
  then
     vcJobdetbgn=`jobdetBegin $vcJobdetid $USER $PASSWORD $SERVER`
     sh ${vcJobcmd} 
     if [ $? -eq 0 ]
     then
       errInfo="Execute Shell Script Successful"
     else
       errInfo="Execute Shell Script Error"
     fi
      ##判断认为执行状态
       vcJobdetbgnStat=`echo "$vcJobdetbgn"|awk -F " " '{print $1}'`
       if [ $vcJobdetbgnStat == "1" ]
				then
 			  #echo "vcJobdetbgn return 1">>job_exit.log
 			  break
			 fi
			 ##判断shell执行结果
       viErrs=`echo "$errInfo"|grep  -e "Error" |wc -l` 
       if  [ $viErrs -ge 1 ]
       then
       vcJobdetEnd=`jobdetEnd $vcJobdetid "$errInfo" 1 $USER $PASSWORD $SERVER`
       else 
       vcJobdetEnd=`jobdetEnd $vcJobdetid "$errInfo" 0 $USER $PASSWORD $SERVER`
       fi
  else 
  ##执行存储过程
    vcJobdetbgn=`jobdetBegin $vcJobdetid $USER $PASSWORD $SERVER`
    vcJobdetbgnStat=`echo "$vcJobdetbgn"|awk -F " " '{print $1}'`
    #echo "vcJobdetbgnStat"$vcJobdetbgnStat
       if [ $vcJobdetbgnStat = "1" ]
				then
 			  echo "vcJobdetbgn return 1">>job_exit.log
 			  break
			 fi
    exeProInfo=`sh DMperformPro.sh $viLastdate "$vcJobcmd" $vcUsrname $vcUsrpwd $vcTnsname`
    #echo "exeProInfo"$exeProInfo
   ## errInfo=`echo "$exeProInfo"|sed s/"\""/""/g|sed s/"'"/""/g|sed s/"\\$"/""/g|awk '{print substr($0,length($0)-2000,length($0))}'`
    errLenVs=$[${#exeProInfo}-2000]
    ##判断返回错误长度
    if [ $errLenVs -ge 0 ]
    then 
    errInfo=${exeProInfo:0-2000}
    else
    errInfo=${exeProInfo:0-${#exeProInfo}}
    fi
    errInfo=`echo "$errInfo"|sed s/"\""/""/g|sed s/"'"/""/g|sed s/"\\$"/""/g|awk '{print $0}'`
    #echo "errLen"${#exeProInfo}
    #echo "errInfo"$errInfo
    viErrs=`echo "$errInfo"|grep  -e "ORA-" -e "AUD-" -e "DRG-" -e "EPC-" -e "IMG-" -e "LCD-" -e "LFI-" -e "MOD-" -e "NCR-" -e "NMP-" -e "NNC-" -e "O2I-" -e  "PGO-" -e "PLS-" -e "PRO-" -e "QSM-" -e "SDO-" -e "Sql*Loader-" -e "TNS-" -e "VID-" -e "TS-0" -e "User-Defined Exception" -e "SP2-"|wc -l`
    echo "vcJobdetid"$vcJobdetid
   if  [ $viErrs -ge 1 ]
       then
       vcJobdetEnd=`jobdetEnd $vcJobdetid "$errInfo" 1 $USER $PASSWORD $SERVER`
       else 
       vcJobdetEnd=`jobdetEnd $vcJobdetid "$errInfo" 0 $USER $PASSWORD $SERVER`
     fi 
  fi    
 ##如果结束任务更新有误，再执行一次 
 vcJobdetEndStat=`echo "$vcJobdetEnd"|awk -F " " '{print $1}'`
   #echo "vcJobdetEndStat"$vcJobdetEndStat
 if [ $vcJobdetEndStat = "1" ]
				then
 			  sleep 20
 			 if  [ $viErrs -ge 1 ]
       then
       vcJobdetEnd=`jobdetEnd $vcJobdetid "$errInfo" 1 $USER $PASSWORD $SERVER`
       else 
       vcJobdetEnd=`jobdetEnd $vcJobdetid "$errInfo" 0 $USER $PASSWORD $SERVER`
       fi 
fi
##如果执行有误，跳出循环
  if [ $viErrs -ge 1 ]
  then
    #echo "i:"$i
    #echo "viErrs_break"$viErrs
    break
  fi
let i=i+1
done
##结束作业
echo "errend"$viErrs
if [ $viErrs -ge 1 ]
  then
    vcJobend=`jobEnd $vcJobid "$errInfo" 1 $USER $PASSWORD $SERVER`
  else
    vcJobend=`jobEnd $vcJobid "$errInfo" 0 $USER $PASSWORD $SERVER`
  fi

vcJobendStat=`echo "$vcJobend"|awk -F " " '{print $1}'`
  #echo "vcJobendStat"$vcJobendStat
 if [ $vcJobendStat = "1" ]
				then
 			  sleep 20
 			 if  [ $viErrs -ge 1 ]
       then
	 vcJobend=`jobEnd $vcJobid "$errInfo" 1 $USER $PASSWORD $SERVER`
       else 
       vcJobend=`jobEnd $vcJobid "$errInfo" 0 $USER $PASSWORD $SERVER`
       fi 
fi
exit 0
