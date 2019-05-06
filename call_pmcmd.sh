# parameter define
batch_date=${1}
src_sys=${2}
src_table_name=${3}
src_ip=${4}
src_port=${5}
src_sid=${6}
src_schema=${7}
src_username=${8}
src_password=${9}
ude_path=${10}
if_mark=${11}

set -v
set -x
# read profile
. ~/.bash_profile

# parameter
folder_name=`sed '/^'"${src_sys}"'=/!d;s/.*=//' $ETL_HOME/script/ext/informatica/call_pmcmd.conf`

batch_year=`echo $batch_date |cut -c1-4`
#find $ETL_HOME/package/template/extract/informatica/paramfile/ -name parameter.${src_sys}_${src_table_name}.*.txt | xargs rm -f 

rm -rf $ETL_HOME/package/template/extract/informatica/paramfile/parameter.${src_sys}_${src_table_name}.txt

if [ ! -f $ETL_HOME/package/template/extract/informatica/paramfile/parameter.${src_sys}_${src_table_name}.txt ] ;then
#    rm -rf $ETL_HOME/package/template/extract/informatica/paramfile/parameter.${src_sys}_${src_table_name}.txt
    paramfile_out_err=`echo -e "[Global]"\\\n"\\$\\$ude_dir=${ude_path}"\\\n"\\$\\$batch_date=${batch_date}"\\\n"\\$\\$src_schema=${src_schema}"\\\n"\\$\\$if_mark=${if_mark}"\\\n"\\$\\$batch_year=${batch_year}" > $ETL_HOME/script/ext/informatica/paramfile/parameter.${src_sys}_${src_table_name}.txt`
fi

mkdir -p ${ude_path}${batch_date}/${src_sys}
chmod 775 ${ude_path}${batch_date}/${src_sys}

#check file
while true
do

dat_out_err=`pmcmd << eof
# DEV
 connect -sv sjtp_is_etl -d INFA950 -u sjpt_test -p "123456"
# SIT
#connect -sv sjpt_is_etl -d Domain_UATINFA950 -u sjpt_uat -p "123456"
# PROD
# connect -sv sjpt_is_etl -d Domain_ETLINFA950 -u sjpt_etl -p "sjpt_etl"
startworkflow -f ${folder_name} -paramfile $ETL_HOME/script/ext/informatica/paramfile/parameter.${src_sys}_${src_table_name}.txt -wait wkf_${src_sys}_${src_table_name}
getworkflowdetails -f ${folder_name} wkf_${src_sys}_${src_table_name}
getsessionstatistics -f ${folder_name} -w wkf_${src_sys}_${src_table_name} ses_${src_sys}_${src_table_name}
exit
eof
`

if [ ! -f ${ude_path}${batch_date}/${src_sys}/${src_sys}_${src_table_name}_${if_mark}.${batch_date}.dat ] ;then
echo "dat file is not created"
sleep 60
else 
echo "dat file had been created"
break 
fi
done

echo -e "$dat_out_err"

job_status=`echo -e "$dat_out_err" | grep "Task run status" | awk -F "[" {'print $2'} | awk -F "]" {'print $1'}`
source_count=`echo -e "$dat_out_err" | grep "Source success rows" | awk -F "[" {'print $2'} | awk -F "]" {'print $1'}`
target_count=`echo -e "$dat_out_err" | grep "Target success rows" | awk -F "[" {'print $2'} | awk -F "]" {'print $1'}`

source_failed_count=`echo -e "$dat_out_err" | grep "Source failed rows" | awk -F "[" {'print $2'} | awk -F "]" {'print $1'}`
target_failed_count=`echo -e "$dat_out_err" | grep "Target failed rows" | awk -F "[" {'print $2'} | awk -F "]" {'print $1'}`

if [ "$source_count" == "" ] ;then
    source_count=0
fi

if [ "$target_count" == "" ] ;then
    source_count=0
fi

#echo -e "++++++++${if_mark}+++++++"


# 2.3
if [ "$source_count" == "$target_count" ] && [ "$job_status" == "Succeeded" ] ;then
    if [ "$source_failed_count" == "$target_failed_count" ] && [ "$source_failed_count" == "0" ] ;then
        rm -f ${ude_path}${batch_date}/${src_sys}/${src_sys}_${src_table_name}_${if_mark}.${batch_date}.bad

	    ok_file="${ude_path}${batch_date}/${src_sys}/${src_sys}_${src_table_name}_${if_mark}.${batch_date}.ok"
	    ok_out_err=`echo "${src_sys}_${src_table_name}="$target_count > $ok_file`
	    ok_result=$?
	
	    if [ "$ok_result" -eq "0" ] ;then
	        echo -e "\nsucceeded create ok file: $ok_file"
	        exit 0
	    else
	        echo -e "\nfailed create ok file: $ok_file"
	        exit $ok_result
	    fi
	  else
    	echo -e "\nexecute wkf_${src_sys}_${src_table_name}: failed"
    	exit 255
    fi
else
    echo -e "\nexecute wkf_${src_sys}_${src_table_name}: failed"
    exit 255
fi





==========================================================================
以下脚本为 runOneObject.sh 的内容
###########################################################################
#!/bin/sh
cd /home/use/opt/PowerCenter/locale

if [ "$#" -lt 2 ];then
echo "Usage $0 [file] "
exit 1
fi

ConfigFile=/home/use/config/informatica2.config

INFORMATICA_USER=`awk 'FS="=" {if($0~/^INFORMATICA_USER/) print $2}' $ConfigFile`
INFORMATICA_PWD=`awk 'FS="=" {if($0~/^INFORMATICA_PWD/) print $2}' $ConfigFile`
INFORMATICA_SERVICE=`awk 'FS="=" {if($0~/^INFORMATICA_SERVICE/) print $2}' $ConfigFile`
INFORMATICA_DOMAIN=`awk 'FS="=" {if($0~/^INFORMATICA_DOMAIN/) print $2}' $ConfigFile`

INFORMATICA_PWD=`/home/use/app/readData $INFORMATICA_PWD`

ETL_DATE=`awk 'FS="=" {if($0~/^ETL_DATE/) print $2}' $ConfigFile`
ETL_DATE="${ETL_DATE:0:4}${ETL_DATE:5:2}${ETL_DATE:8:2}"
informatica_file=$1
workflowName=$2;

if [ ! -d /home/use/log_informatica/"$ETL_DATE" ]
then
	mkdir /home/use/log_informatica/"$ETL_DATE"
  chmod 777 /home/use/log_informatica/"$ETL_DATE"
fi

if [ -f /home/use/log_informatica/"$ETL_DATE"/"$workflowName".log ]
then
	rm -f /home/use/log_informatica/"$ETL_DATE"/"$workflowName".log
fi

pmcmd startworkflow -service $INFORMATICA_SERVICE -d $INFORMATICA_DOMAIN -u $INFORMATICA_USER -p $INFORMATICA_PWD -f $informatica_file -paramfile /home/use/config/informatica.config -wait $workflowName

pmcmd getworkflowdetails -service $INFORMATICA_SERVICE -d $INFORMATICA_DOMAIN -u $INFORMATICA_USER -p $INFORMATICA_PWD -f $informatica_file  $workflowName > /home/use/log_informatica/"$ETL_DATE"/"$workflowName".log

#status=`sed -n '/工作流运行状态/'p /home/use/log_informatica/"$ETL_DATE"/"$workflowName".log | awk '{print$2}'`    
status=`sed -n '/Workflow run status/'p /home/use/log_informatica/"$ETL_DATE"/"$workflowName".log | awk '{print$4}'`
#if [ $status = "[成功]" ]


if [ $status = "[Succeeded]" ]
then    
	echo "$(date '+%Y-%m-%d %H:%M:%S') $workflowName鎴愬姛锛屾墽琛屽畬姣?"
	exit 0 
else
	echo "$(date '+%Y-%m-%d %H:%M:%S') $workflowName鍑洪敊锛岃鏌ョ湅璇︾粏鏃ュ織 "
	if [ ! -d /home/use/log_informatica_error/"$ETL_DATE" ]
    then
	    mkdir /home/use/log_informatica_error/"$ETL_DATE"
        chmod 777 /home/use/log_informatica_error/"$ETL_DATE"
    fi
	ti=`date "+%Y%m%d%H%M%S"`
	pmcmd gettaskdetails -service $INFORMATICA_SERVICE -d $INFORMATICA_DOMAIN -u $INFORMATICA_USER -p $INFORMATICA_PWD -f $informatica_file  $workflowName > /home/use/log_informatica_error/"$ETL_DATE"/"$workflowName"_$ti.log
	exit 1   
fi
