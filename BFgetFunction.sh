#!/bin/bash
###########################################################
## Program Name: getFunction
## Created Date: 2017-06-27
## Author  : 米辉翀
## Cotent:function for mainCtrJob.sh
############################################################


##################################################
#功能:判断是否启动调度
#作者:米辉翀
#参数:<用户><密码><地址>
##################################################

function isStart
{
USER=$1
PASSWORD=$2
SERVER=$3
SQLSTR=`sqlplus -s ${USER}/${PASSWORD}@${SERVER} <<EOF
SET COLSEP ' ';
SET ECHO  OFF;
SET FEEDBACK OFF;
SET HEADING OFF;
SET LINESIZE 1000;
SET NUMWIDTH 20;
SET PAGESIZE 0;
SET TERMOUT OFF;
SET TRIMSPOOL ON;
SET TRIMOUT OFF;
SET VERIFY OFF;
SET ARRAYSIZE 5000;
SET SQLBLANKLINE OFF;
alter session set nls_date_format='YYYY-MM-DD hh:mi:ss';
select cd_value from ct_main_code_info where cd_ename='IS_START' and cd_stat='1';
EXIT
EOF`;
  echo $SQLSTR
}



##################################################
#功能:获取执行作业
#作者:米辉翀
#参数:<用户><密码><地址>
##################################################

function getJob
{
USER=$1
PASSWORD=$2
SERVER=$3
SQLSTR=`sqlplus -s ${USER}/${PASSWORD}@${SERVER} <<EOF
SET COLSEP ' ';
SET ECHO  OFF;
SET FEEDBACK OFF;
SET HEADING OFF;
SET LINESIZE 1000;
SET NUMWIDTH 20;
SET PAGESIZE 0;
SET TERMOUT OFF;
SET TRIMSPOOL ON;
SET TRIMOUT OFF;
SET VERIFY OFF;
SET ARRAYSIZE 5000;
SET SQLBLANKLINE OFF;
alter session set nls_date_format='YYYY-MM-DD hh:mi:ss';
variable p_result varchar2(4000);
execute BF_JOB_CTRL.get_my_job($4,:p_result);
select :p_result from dual;
EXIT
EOF`;
  echo $SQLSTR
}


##################################################
#功能:作业执行开始
#作者:米辉翀
#参数:<JOBID> <用户><密码><地址>
##################################################
function jobBegin
{
 USER=$2
 PASSWORD=$3
 SERVER=$4
  SQLSTR=`sqlplus -s ${USER}/${PASSWORD}@${SERVER} <<EOF
SET COLSEP ' ';
SET ECHO  OFF;
SET FEEDBACK OFF;
SET HEADING OFF;
SET LINESIZE 1000;
SET NUMWIDTH 20;
SET PAGESIZE 0;
SET TERMOUT OFF;
SET TRIMSPOOL ON;
SET TRIMOUT OFF;
SET VERIFY OFF;
SET ARRAYSIZE 5000;
SET SQLBLANKLINE OFF;
alter session set nls_date_format='YYYY-MM-DD hh:mi:ss';
variable p_up_status number;
execute  BF_JOB_CTRL.up_job_begin($1,:p_up_status);
select :p_up_status from dual;
EXIT
EOF`;
  echo $SQLSTR
}


##################################################
#功能:作业执行完毕
#作者:米辉翀
#参数:<JOBID> <状态><用户><密码><地址>
##################################################

function jobEnd
{
 USER=$3
 PASSWORD=$4
 SERVER=$5
  SQLSTR=`sqlplus -s ${USER}/${PASSWORD}@${SERVER} <<EOF
SET COLSEP ' ';
SET ECHO  OFF;
SET FEEDBACK OFF;
SET HEADING OFF;
SET LINESIZE 1000;
SET NUMWIDTH 20;
SET PAGESIZE 0;
SET TERMOUT OFF;
SET TRIMSPOOL ON;
SET TRIMOUT OFF;
SET VERIFY OFF;
SET ARRAYSIZE 5000;
SET SQLBLANKLINE OFF;
alter session set nls_date_format='YYYY-MM-DD hh:mi:ss';
variable p_up_status number;
execute  BF_JOB_CTRL.up_job_end($1,$2,'$6',:p_up_status);
select :p_up_status from dual;
EXIT
EOF`;
  echo $SQLSTR
}


##################################################
#功能:查询外部表,测试是否有误
#作者:米辉翀
#参数:<用户><密码><地址><JOBid>
##################################################

function getLoadTabStat
{
USER=$1
PASSWORD=$2
SERVER=$3
SQLSTR=`sqlplus -s ${USER}/${PASSWORD}@${SERVER} <<EOF
SET COLSEP ' ';
SET ECHO  OFF;
SET FEEDBACK OFF;
SET HEADING OFF;
SET LINESIZE 1000;
SET NUMWIDTH 20;
SET PAGESIZE 0;
SET TERMOUT OFF;
SET TRIMSPOOL ON;
SET TRIMOUT OFF;
SET VERIFY OFF;
SET ARRAYSIZE 5000;
SET SQLBLANKLINE OFF;
alter session set nls_date_format='YYYY-MM-DD hh:mi:ss';
variable p_qry_status number;
execute BF_JOB_CTRL.get_load_tab_stat($4,:p_qry_status);
select :p_qry_status from dual;
EXIT
EOF`;
  echo $SQLSTR
}



##################################################
#功能:从源字符串中截取开始串及结束串包围的部分
#作者:米辉翀
#参数:<源字符串> <开始串> <结束串>
##################################################
function getStr
{
case $# in
3)
   echo $1|awk '{
      SourceStr=$0;
      strBegin="'${2}'";
      strEnd="'${3}'";
      lngBeginLen=length(strBegin);
      lngBegin=index(SourceStr, strBegin);
      lngEndLen=length(strEnd);
      if(lngEndLen==0)
         lngEnd=length(SourceStr)+1;
      else
      {  lngCut=lngBegin+lngBeginLen+1;
         SourceStr1=substr(SourceStr,lngCut);
         lngEnd=index(SourceStr1,strEnd)+lngCut-1;}
      print substr(SourceStr,lngBegin+lngBeginLen,lngEnd-lngBegin-lngBeginLen); 
      }'
   ;;
*)
        echo "[$0][参数:<源字符串> <开始串> <结束串>"
        exit 0
   ;;
esac
}


##################################################
#功能:从XML字符串中截取内容
#作者:米辉翀
#参数:<源字符串> <XML标记>
##################################################
function getXmls
{
case $# in
2)
        SrcStr=$1
        XmlSign=$2
        LeftXml="<"$XmlSign">"
        RightXml="</"$XmlSign">"
        getStr "$SrcStr" "$LeftXml" "$RightXml"
   ;;
*)
        echo "[$0][参数:<源字符串> <XML标记>"
        exit 0
   ;;
esac
}


##################################################
#功能:SFTP获取文件
#作者:米辉翀
#参数:<用户名> <密码><地址><文件路径><文件名>
##################################################
function getFile
{
USER=$1
PASSWORD=$2
SERVER=$3
lftp -u $USER,$PASSWORD sftp://$SERVER <<EOF
lcd /home/oracle/odsdata
get ${4}/${5}
by
EOF
}




