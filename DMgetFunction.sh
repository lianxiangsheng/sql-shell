#!/bin/sh
##########################################################
# Program Name: getFunction
# Created Date: 2017-07-24
#     Author  : 米辉翀
#     Cotent:function for DMmainCtrCen.sh
##########################################################



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
#1|?:???′???
#??:??? 
#2???§><??><μ?·><·t?ID>(?3???мˉ?μ??
##################################################
function getJobdet
{
USER=$1
PASSWORD=$2
SERVER=$3
JOBPART=$4
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
SELECT DM_JOB_CTRL.get_my_job_det(${JOBPART}) FROM DUAL;
EXIT
EOF`;
  echo $SQLSTR
}

##################################################
#1|?:?a???
#??:??? 
#2???μID><??§><??><μ?·>
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
variable p_ret_status number;
execute  DM_JOB_CTRL.job_begin($1,:p_ret_status);
select :p_ret_status from dual;
EXIT
EOF`;
  echo $SQLSTR
}

##################################################
#1|?:?a???(?′???)
#??:??? 
#2???肾<??§><??><μ?·>
##################################################
function jobdetBegin
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
variable p_ret_status number;
execute  DM_JOB_CTRL.jobdet_begin($1,:p_ret_status);
select :p_ret_status from dual;
EXIT
EOF`;
  echo $SQLSTR
}

##################################################
#1|?:????
#??:??? 
#2???μID><2??????±?:0?3￡1?°?<??§><??><μ?·>
##################################################

function jobEnd
{
 USER=$4
 PASSWORD=$5
 SERVER=$6
 SQLSTR=`sqlplus -s ${USER}/${PASSWORD}@${SERVER} <<EOF
set serveroutput on
declare
p_ret_status number;
p_opt_res varchar2(4000);
begin
p_opt_res:='$2';
 DM_JOB_CTRL.job_end($1,p_opt_res,$3,p_ret_status);
 dbms_output.put_line(p_ret_status);
exception
  when others then
    dbms_output.put_line(sqlerrm);
end;
/
EXIT
EOF`;
  echo $SQLSTR
}

##################################################
#1|?:????
#??:??? 
#2???肾<2??????±?:0?3￡1?°?<??§><??><μ?·>
##################################################

function jobdetEnd
{
 USER=$4
 PASSWORD=$5
 SERVER=$6
 SQLSTR=`sqlplus -s ${USER}/${PASSWORD}@${SERVER} <<EOF
set serveroutput on
declare
p_ret_status number;
p_opt_res varchar2(4000);
begin
p_opt_res:='$2';
DM_JOB_CTRL.jobdet_end($1,p_opt_res,$3,p_ret_status);
 dbms_output.put_line(p_ret_status);
exception
  when others then
    dbms_output.put_line(sqlerrm);
end;
/
EXIT
EOF`;
  echo $SQLSTR
}


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
        echo "[$0][2??????<?a?′?> <??′?>"
        exit 0
   ;;
esac
}


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
        echo "[$0][2??????<XML±?>"
        exit 0
   ;;
esac
}

function jobruninterval
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
select job_run_interval from ct_job_info where job_id=$4;
EXIT
EOF`;
  echo $SQLSTR
}

function lastdatdate
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
alter session set nls_date_format='YYYY-MM-DD hh24:mi:ss';
select last_dat_date from ct_job_info where job_id=$4;
EXIT
EOF`;
  echo $SQLSTR
}


function updatelastdatdate
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
alter session set nls_date_format='YYYY-MM-DD hh24:mi:ss';
update ct_job_info set last_dat_date = last_dat_date + $5 where job_id=$4;
update ct_job_info set run_stat=0 where job_id=$4;
update ct_job_det_info set  run_stat=0 where job_id=$4;
commit;
EXIT
EOF`;
  echo $SQLSTR
}

