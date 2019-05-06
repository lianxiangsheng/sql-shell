create or replace package body odsdata.BF_JOB_CTRL is
   /******************************************************************************
    * TITLE      : 广州银行数据集市-数据装载调度包
    * DESCRIPTION:数据装载调度包
    * AUTHOR     : 米辉翀
    * VERSION    : 1.0
    * DATE       : 2017-06-26
    2017-11-13  米辉翀，添加对沉淀层的判断,防止缓冲层追数或者沉淀层由于异常未及时装数缓冲层就翻牌的情况
    2017-11-27  米辉翀，添加忽略检查配置(返还文件一致有BAD数据的个别情况)
   ******************************************************************************
    ******************************************************************************/


   --返回待执行的JOBID
    procedure get_my_job(p_job_part in number,p_result out varchar2) as
   v_conf_id NUMBER;
   v_lc_tab_ename VARCHAR2(50);
   v_serv_addr VARCHAR2(100);
   v_serv_user VARCHAR2(50);
   v_serv_pwd VARCHAR2(100);
   v_remote_file_name varchar2(100);
   v_remote_file_path varchar2(100);
  -- v_is_start varchar2(50);
  -- v_ACTIVE_PARALLEL varchar2(50);
   v_ACTIVE_PARALLEL_n number;
   v_MAX_PARALLEL varchar2(50);
   v_MAX_PARALLEL_n number;
    v_step_name   varchar2(100);
    v_ERR_RETRY_TIME  varchar2(50);
    vc_err_time integer;
     v_opt_log_id  number;
     v_suc_num integer;
   begin
   /*   v_step_name:='判断调度是否启用 ,如果未启用，返回';
    VERS_TOOLS.get_code_value('IS_START',v_is_start);
    if(v_is_start ='0') then return null; end if;
   v_step_name:='判断是否在活动,如果正在活动，返回';
    VERS_TOOLS.get_code_value('IS_ACTIVE',v_IS_ACTIVE);
    if(v_IS_ACTIVE ='1') then return null ; end if;*/
     p_result:='';
    v_step_name:='判断并发数，如果达到最大并发数，返回 ';
   --设置当前并行数
   select count(1) into v_ACTIVE_PARALLEL_n from CT_LOAD_CONF_INFO where RUN_STATUS='1';

    --获取最大并行数
    VERS_TOOLS.get_code_value('MAX_PARALLEL',v_MAX_PARALLEL);
    v_MAX_PARALLEL_n:=to_number(v_MAX_PARALLEL);
     if(v_ACTIVE_PARALLEL_n>=v_MAX_PARALLEL_n) then p_result:=''; return  ; end if;
     -----------------------------------------
   --开始执行
    VERS_TOOLS.set_code_value('ACTIVE_TIME',to_char(sysdate,'yyyymmdd hh24:mi:ss'));

    v_step_name:='将成功的更新为待执行';
    select count(1) into v_suc_num from CT_LOAD_CONF_INFO where  status='1' and  run_status=2 and JOB_PART=p_job_part;
   if(v_suc_num>=1) then
    update CT_LOAD_CONF_INFO t set  t.last_dat_date= trunc(case when t.dat_date_add='D' then t.last_dat_date+1 when t.dat_date_add='M' then add_months(last_day(t.last_dat_date),1)  when t.dat_date_add='Y' then  add_months(to_date(to_char(t.last_dat_date,'yyyy')||'1231','yyyymmdd'),12) else sysdate end ,'dd') ,t.run_status=0 where  status='1' and  t.run_status=2 and JOB_PART=p_job_part;
   commit;
   end if;

   v_step_name:='获取重试次数';
   VERS_TOOLS.get_code_value('ERR_RETRY_TIME',v_ERR_RETRY_TIME);
   vc_err_time:=to_number(nvl(v_ERR_RETRY_TIME,0));
    v_step_name:='获取执行作业';
    --优先级高，通过SQL判断依赖的，错误次数少，活动时间早的先执行

 select  conf_id,lc_tab_ename,serv_addr,serv_user,serv_pwd,remote_file_path,remote_file_name
  into v_conf_id,v_lc_tab_ename,v_serv_addr,v_serv_user,v_serv_pwd,v_remote_file_path,v_remote_file_name
  from (
   select a.conf_id,
       a.lc_tab_ename,
       replace( replace(replace(replace(replace(a.rely_id,
                  '$[YYYYMMDD]$',
                  to_char(a.LAST_DAT_DATE, 'yyyymmdd')),'$[YYYY-MM-DD]$',to_char(a.LAST_DAT_DATE, 'yyyy-mm-dd')),
                  '$[YYYYMM]$',to_char(a.LAST_DAT_DATE, 'yyyymm')),
                   '$[YYYY-MM]$',to_char(a.LAST_DAT_DATE, 'yyyy-mm')),
                   '$[YYYY]$',to_char(a.LAST_DAT_DATE, 'yyyy'))   rely_id,
       b.serv_ename,
       b.serv_addr,
       b.serv_user,
       bf_job_ctrl.get_dec_val(b.serv_pwd) serv_pwd,
       a.LAST_DAT_DATE,
        replace( replace(replace(replace(replace(a.remote_file_path ,
                  '$[YYYYMMDD]$',
                  to_char(a.LAST_DAT_DATE, 'yyyymmdd')),'$[YYYY-MM-DD]$',to_char(a.LAST_DAT_DATE, 'yyyy-mm-dd')),
                  '$[YYYYMM]$',to_char(a.LAST_DAT_DATE, 'yyyymm')),
                   '$[YYYY-MM]$',to_char(a.LAST_DAT_DATE, 'yyyy-mm')),
                   '$[YYYY]$',to_char(a.LAST_DAT_DATE, 'yyyy'))    remote_file_path,
          replace( replace(replace(replace(replace( a.remote_file_name,
                  '$[YYYYMMDD]$',
                  to_char(a.LAST_DAT_DATE, 'yyyymmdd')),'$[YYYY-MM-DD]$',to_char(a.LAST_DAT_DATE, 'yyyy-mm-dd')),
                  '$[YYYYMM]$',to_char(a.LAST_DAT_DATE, 'yyyymm')),
                   '$[YYYY-MM]$',to_char(a.LAST_DAT_DATE, 'yyyy-mm')),
                   '$[YYYY]$',to_char(a.LAST_DAT_DATE, 'yyyy'))    remote_file_name,
        row_number() over(order by a.PRIOR_LV       desc,
          a.err_retry_time asc,
          a.active_time asc) rn
  from
  v_CT_LOAD_CONF_INFO a, v_ct_src_serv_info b
 where a.serv_ename = b.serv_ename 
 and a.STATUS='1' 
   and  bf_job_ctrl.getlean_flg(conf_id)=1
   and a.JOB_PART=p_job_part
   and (a.run_status in ('0', '3') or
       (a.run_status = -1 and a.err_retry_time <vc_err_time) ) )  where rn=1  ;


    v_step_name:='更新获取作业的状态';
    update CT_LOAD_CONF_INFO t set t.ACTIVE_TIME=sysdate where t.conf_id=v_conf_id;
    commit;


     p_result:=p_result||'<CONF_ID>'||v_conf_id||'</CONF_ID><LC_TAB_ENAME>'||v_lc_tab_ename||'</LC_TAB_ENAME><SERV_ADDR>'||v_SERV_ADDR||'</SERV_ADDR><SERV_USER>'
    ||v_SERV_USER||'</SERV_USER><SERV_PWD>'||v_SERV_PWD||'</SERV_PWD><REMOTE_FILE_PATH>'||v_remote_file_path||'</REMOTE_FILE_PATH><REMOTE_FILE_NAME>' ||v_remote_file_name||'</REMOTE_FILE_NAME>';
    exception
    when no_data_found then
     p_result:='';
    return ;
    when others then
      dbms_output.put_line(sqlerrm);
       VERS_TOOLS.opt_db_begin('ODS_JOB_CTRL.get_my_job', v_conf_id||'.'||v_step_name,null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'调度异常'||sqlerrm,1);
       p_result:='';
      return ;
   end;


--返回加密串,用于加密
  FUNCTION get_enc_val(p_in_val IN VARCHAR2)
  RETURN VARCHAR2 IS
  p_key     VARCHAR2(64) := '22055235F4AF98538A48F051F5FEACFE';
  p_iv      VARCHAR2(8) := '00000000';
  BEGIN

  RETURN VERS_TOOLS.get_enc_val(p_in_val,p_key,p_iv);
  end get_enc_val;

  --返回解密字符串，用于解密
  FUNCTION get_dec_val(p_in_val IN VARCHAR2)
  RETURN VARCHAR2 IS
   p_key     VARCHAR2(64) := '22055235F4AF98538A48F051F5FEACFE';
  p_iv      VARCHAR2(8) := '00000000';
BEGIN
  RETURN VERS_TOOLS.get_dec_val(p_in_val,p_key,p_iv);
  end get_dec_val;
  --作业开始，更新数据，参数一： CONF_ID,参数二：返回标识 0 正常 1 异常
  procedure up_job_begin(p_job_id  number,p_up_status out number ) as
   v_opt_log_id  number;
  begin
   update CT_LOAD_CONF_INFO t set t.run_begin_time=sysdate,t.run_status=1 where t.conf_id=p_job_id;
   if sql%rowcount=0 then
      p_up_status:=1;
    return;
   end if;
   commit;
   p_up_status :=0;
  exception
 when others then
      dbms_output.put_line(sqlerrm);
      VERS_TOOLS.opt_db_begin('ODS_JOB_CTRL.up_job_begin', p_job_id||'.'||'作业开始状态更新',null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'调度异常'||sqlerrm,1);
      p_up_status :=1;
  end up_job_begin;


  procedure up_job_end(p_job_id  number,p_flag number,p_run_res varchar2,p_up_status out number) as
   v_opt_log_id  number;
  begin
  --正常结束
  if (p_flag=0) then
   update CT_LOAD_CONF_INFO t set t.run_end_time=sysdate,t.run_status=2,t.run_res=p_run_res,t.err_retry_time=0  where t.conf_id=p_job_id;
   --异常结束
  elsif(p_flag=1) then
   update CT_LOAD_CONF_INFO t set t.run_end_time=sysdate,t.run_status=-1,t.err_retry_time=nvl(err_retry_time,0)+1,t.run_res=p_run_res where t.conf_id=p_job_id;
  end if;
   if sql%rowcount=0 then
      p_up_status:=1;
    return;
   end if;
   commit;
    p_up_status:=0;
   exception
 when others then
      dbms_output.put_line(sqlerrm);
      VERS_TOOLS.opt_db_begin('ODS_JOB_CTRL.up_job_end', p_job_id||'.'||'作业结束状态更新',null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'调度异常'||sqlerrm,1);
         p_up_status:=1;
  end up_job_end;


   --返回所依赖的作业是否已完成，是返回1 否返回0
  function getlean_flg(p_jobid integer) return integer is
	--pragma autonomous_transaction;
  v_cnt integer;
  v_rely_id VARCHAR2(100);
  v_job_date date;--作业日期
  v_lean_date date;--依赖日期
  v_rely_sql VARCHAR2(2000);--依赖SQL
  v_opt_log_id number;
  v_tab_name VARCHAR2(50);
  v_dw_date date;--沉淀层数据日期
  begin

  --判断是否存在此作业
   select count(1) into v_cnt from CT_LOAD_CONF_INFO where CONF_ID=p_jobid;
    if(v_cnt=0) then
      return 0;  --无此jobid 直接返回 - 0
    end if;
    select a.RELY_ID,a.last_dat_date,b.rely_sql,a.lc_tab_ename into v_rely_id,v_job_date,v_rely_sql,v_tab_name from CT_LOAD_CONF_INFO a ,CT_SRC_SERV_INFO b
     where a.serv_ename=b.serv_ename and  a.CONF_ID=p_jobid ;

     if(v_rely_id is not null and v_rely_sql is not null ) then
     --SQL 获取依赖作业日期
     v_rely_sql:=replace(v_rely_sql,'$[RELY_ID]$',v_rely_id);
		 --dbms_output.put_line(v_rely_sql);
     execute immediate v_rely_sql into v_lean_date;
     else
      return 0;
     end if;
     --添加对沉淀层的判断,防止缓冲层追数或者沉淀层由于异常未及时装数，缓冲层就翻牌的情况
      select count(1) into v_cnt from odsbdata.ct_mv_local_tab_sync t where t.rt_tab_ename=upper(v_tab_name) and status=1;
    if(v_cnt>=1) then
      select min(last_dat_date) into  v_dw_date from odsbdata.ct_mv_local_tab_sync t where t.rt_tab_ename=upper(v_tab_name);
    end if;
    if(v_cnt>=1 and (v_dw_date is null or v_dw_date<v_job_date) ) then 
    return 0;
    end if;
     --依赖日期大于等于作业日期,则满足依赖条件
     --去掉判断  and (v_dw_date is null or  v_dw_date>=v_job_date)
     if(v_lean_date is not null and v_job_date is not null and v_lean_date-v_job_date>0 ) then
     return 1;
     else
     return 0;
     end if;
       exception
  when others then

      dbms_output.put_line(sqlerrm);
      VERS_TOOLS.opt_db_begin('ODS_JOB_CTRL.getlean_flg', p_jobid||'.'||v_rely_sql||'.判断依赖是否满足',null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'调度异常,relysql:'||v_rely_sql||sqlerrm,1);
      return 0;
  end;

  --创建外部表  1创建 0 未创建或创建失败
  function get_ext_tab_sql(p_job_id  number) return clob is
  v_cnt integer;
  v_opt_log_id number;
  v_tab_name varchar2(50);--表名
  v_file_sep VARCHAR2(5);--分隔符
  v_tab_cols varchar2(4000);--建表字段
  v_sql clob;
  v_stepname varchar2(100);
  v_cols varchar2(4000);
  v_clos_sp VERS_TOOLS.typ_varchar2;
  v_colum varchar2(50);--字段名
  v_colum_typ varchar2(50);--字段类型
  begin
  v_stepname:='判断是否有此作业';
  select count(1) into v_cnt from CT_LOAD_CONF_INFO where CONF_ID=p_job_id;
    if(v_cnt=0) then
       return '';
    end if;
   v_stepname:='赋值';
  select  t.lc_tab_ename,t.remote_file_sep,t.tab_cols into v_tab_name,v_file_sep,v_tab_cols from CT_LOAD_CONF_INFO t  where CONF_ID=p_job_id;
  v_stepname:='判断该外部表是否已存在';
/*  v_cnt:=0;
  select vers_tools.is_ext_tab_exists(upper(v_tab_name)) into v_cnt from dual;
   if(v_cnt>=1) then
      vers_tools.drop_tab_no_interruption(v_tab_name);
    end if;*/

   v_clos_sp := VERS_TOOLS.split(v_tab_cols, ',');
    FOR item IN v_clos_sp.first .. v_clos_sp.last LOOP
   v_colum:= ltrim(v_clos_sp(item));
   v_colum_typ:= UPPER(TRIM(substr(v_colum, instr(v_colum, ' ')))) ;
   v_colum:= substr(v_colum, 1, instr(v_colum, ' ')- 1) ;
     IF instr(v_colum_typ, 'DATE') > 0
    THEN
      v_colum:=v_colum||' DATE "MM/DD/YYYY HH24:MI:SS"';
    END IF;
   v_cols := CASE
                 WHEN v_cols IS NOT NULL THEN
                  v_cols || ' , '
                 ELSE
                  v_cols
               END ||v_colum;
    END LOOP;
   v_cols :=rtrim(trim(replace(v_cols,',  ,',',')),',');
    v_stepname:='建表';
  v_sql:='create table '||v_tab_name||'('||v_tab_cols||')'||'organization external(
  type oracle_loader
  default directory odsdata_dir
  access parameters (
  records delimited by newline
  logfile ''ODSLOAD_LOG_DIR'':'''||v_tab_name||'.log'''||'
  badfile ''ODSLOAD_LOG_DIR'':'''||v_tab_name||'.bad'''||'
  fields terminated by '||v_file_sep||'
	missing field values are null
  ( '||v_cols||' )
  )  location ('''||v_tab_name||'.DAT'''||')) reject limit unlimited';
  --execute immediate v_sql;
  --字段长度默认255，如有超过的需要显示描述
  --日期字段需要注明格式，可参考表BF_IMS_KNC_CORP
 return  v_sql;
    exception
  when others then
      dbms_output.put_line(sqlerrm);
      VERS_TOOLS.opt_db_begin('ODS_JOB_CTRL.create_ext_tab', p_job_id||'.'||dbms_lob.substr(v_sql,1,3000)||'.'||v_stepname,null, v_opt_log_id);
     -- VERS_TOOLS.opt_db_begin('ODS_JOB_CTRL.create_ext_tab', p_job_id||'.'||v_sql||'.'||v_stepname,null, v_opt_log_id);
      VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'建表失败'||sqlerrm,1);

  end;
--1查询正常 0查询有误
procedure get_load_tab_stat(p_job_id  number,p_qry_status out number)  as
  v_cnt integer;
  v_opt_log_id number;
  v_tab_name varchar2(50);--表名
  v_sql varchar2(2000);
  v_stepname varchar2(100);
  v_is_check varchar2(1);--是否检查
  begin
  v_stepname:='判断是否有此作业';
  select count(1) into v_cnt from CT_LOAD_CONF_INFO where CONF_ID=p_job_id;
    if(v_cnt=0 ) then
       p_qry_status :=0;
    end if;
   v_stepname:='赋值';
  select  t.lc_tab_ename,t.is_ignore_check into v_tab_name,v_is_check from CT_LOAD_CONF_INFO t  where CONF_ID=p_job_id;
   v_stepname:='判断该外部表是否需检查';
   if(v_is_check='1') then
       p_qry_status :=1;
       return;
    end if; 
  v_stepname:='判断该外部表是否已存在';
  v_cnt:=0;
  select vers_tools.is_ext_tab_exists(upper(v_tab_name)) into v_cnt from dual;
   if(v_cnt=0) then
       p_qry_status :=0;
    end if;
v_cnt:=0;
 v_stepname:='查询数据条数';
  v_sql:='select count(1)  from   '||v_tab_name||'';
execute immediate v_sql into v_cnt;
 p_qry_status :=1;
    exception
  when others then
      dbms_output.put_line(sqlerrm);
      VERS_TOOLS.opt_db_begin('ODS_JOB_CTRL.get_load_tab_num', p_job_id||'.'||dbms_lob.substr(v_sql,1,3000)||'.'||v_stepname,null, v_opt_log_id);
      VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'查询外部表有误'||sqlerrm,1);
     p_qry_status :=0;
  end;



    end BF_JOB_CTRL;
/
