create or replace package body odsbdata.MVIEW_MANAGE
is


  --全局变量->开始
  C_PAG_NAME varchar(30) := 'MVIEW_MANAGE';
  C_MAIN_VERSION varchar(10) := 'v1.0.0';
  C_LOG_TITLE varchar(64) := C_PAG_NAME||C_MAIN_VERSION;
   --全局变量->结束


  --主程序->开始
  procedure main_do (p_job_part in number,p_status out number) as
    v_opt_log_id number;
    v_prg_name varchar(30) := 'MAIN_DO';
    v_dblog_title varchar(256) := C_LOG_TITLE||'.'||v_prg_name;
    v_step_name varchar(256);
   -- v_ACTIVE_PARALLEL varchar2(50);
    v_MAX_PARALLEL varchar2(50);
     v_ACTIVE_PARALLEL_n number;
    v_MAX_PARALLEL_n number;
    v_ERR_RETRY_TIME varchar2(50);
    v_mvlts_id integer;
    v_suc_num integer;
    vc_ERR_RETRY_TIME number;
    v_begin_status number;
  begin
   v_step_name:='判断并发数，如果达到最大并发数，返回 ';
   --获取当前并行数
   select count(1) into v_ACTIVE_PARALLEL_n from ct_mv_local_tab_sync where RUN_STATUS='1';

    --获取最大并行数
    VERS_TOOLS.get_code_value('MAX_PARALLEL',v_MAX_PARALLEL);
      v_MAX_PARALLEL_n:=to_number(v_MAX_PARALLEL);
     if(v_ACTIVE_PARALLEL_n>=v_MAX_PARALLEL_n) then p_status:=1; return ; end if;
     -----------------------------------------
   --开始执行

    v_step_name:='将成功的更新为待执行';
    select count(1) into v_suc_num from ct_mv_local_tab_sync where status='1' and run_status=2 and JOB_PART=p_job_part;
   if(v_suc_num>=1) then

   update ct_mv_local_tab_sync t set  t.last_dat_date=trunc( case when upper(t.dat_date_add)='D' then t.last_dat_date+1 when upper(t.dat_date_add)='M' then add_months(last_day(t.last_dat_date),1)  when upper(t.dat_date_add)='Q' then add_months(trunc(t.last_dat_date,'q'),6)-1 when upper(t.dat_date_add)='Y' then  add_months(to_date(to_char(t.last_dat_date,'yyyy')||'1231','yyyymmdd'),12) when  t.dat_date_add is null and t.run_interval is not null then  nvl(t.RUN_END_TIME,t.last_dat_date)+t.run_interval else sysdate end ,'dd'),t.run_status=0
   where   status='1' and  t.run_status=2 and JOB_PART=p_job_part;
   commit;

   end if;
   v_step_name:='获取重试次数';
   VERS_TOOLS.get_code_value('ERR_RETRY_TIME',v_ERR_RETRY_TIME);
   vc_ERR_RETRY_TIME:=to_number(nvl(v_ERR_RETRY_TIME,0));
    v_step_name:='获取执行作业';

    select mvlts_id into v_mvlts_id from
    (select  t.mvlts_id, row_number() over(order by t.PRIOR_LV       desc,
          t.curr_rowcount asc,
          t.err_retry_time asc,
          t.active_time asc) rn
     from   (select mvlts_id,PRIOR_LV,curr_rowcount,err_retry_time,active_time, MVIEW_MANAGE.getlean_flg(mvlts_id) is_lean
     from v_ct_mv_local_tab_sync  where ( run_status in (0, 3)  or (run_status = -1 and err_retry_time <vc_ERR_RETRY_TIME) ) and JOB_PART=p_job_part
     ) t
  where is_lean=1
   ) where rn=1    ;

   ----开始同步数据,更新执行状态
   up_job_begin(v_mvlts_id,v_begin_status );
   if(v_begin_status <> 0) then
   return;
   end if;
   
 /*   v_step_name:='更新当前并发数';
  v_ACTIVE_PARALLEL:=to_char(v_ACTIVE_PARALLEL_n+1);
   VERS_TOOLS.set_code_value('ACTIVE_PARALLEL',v_ACTIVE_PARALLEL);*/

   --进入同步程序
   VERS_TOOLS.set_code_value('ACTIVE_TIME',to_char(sysdate,'yyyymmdd hh24:mi:ss'));
   mv_tab_sync(v_mvlts_id);
   p_status:=0;
  exception
   when no_data_found then
   p_status:=1;
    return ;
   when others then
      dbms_output.put_line(sqlerrm);
       VERS_TOOLS.opt_db_begin(v_dblog_title, '同步主控程序'||'.'||v_step_name,null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'调度异常'||sqlerrm,1);
       p_status:=1;
      return ;

  end main_do;
  --主程序->结束


 --获取依赖作业执行情况-->开始
function getlean_flg(p_jobid integer) return integer is
pragma autonomous_transaction;
 v_cnt integer;
  v_rely_id integer;
  v_job_date date;--作业日期
  v_lean_date date;--依赖日期
  v_rely_sql VARCHAR2(2000);--依赖SQL
  v_opt_log_id number;
   v_prg_name varchar(30) := 'getlean_flg';
    v_dblog_title varchar(256) := C_LOG_TITLE||'.'||v_prg_name;
    v_run_interval number;
    v_run_end_time date;
  begin
  --判断是否存在此作业
 -- dbms_output.put_line('jobid:'||p_jobid);
   select count(1) into v_cnt from CT_MV_LOCAL_TAB_SYNC where mvlts_id=p_jobid ;
    if(v_cnt=0) then
      return 0;  --无此jobid 直接返回 - 0
    end if;
 select a.RELY_ID,a.last_dat_date,a.run_interval,a.run_end_time,b.rely_sql into v_rely_id,v_job_date,v_run_interval,v_run_end_time,v_rely_sql from CT_MV_LOCAL_TAB_SYNC a
 left join CT_SRC_SERV_INFO b on  a.RELY_SYS=b.serv_ename
  where   a.mvlts_id=p_jobid ;
 if(v_rely_id is not null and v_rely_sql is not null ) then
     --SQL 获取依赖作业日期
     v_rely_sql:=replace(v_rely_sql,'$[RELY_ID]$',v_rely_id);
     execute immediate v_rely_sql into v_lean_date;
     --定时同步
  elsif (trim(v_rely_id) is null and nvl(v_run_end_time,v_job_date-nvl(v_run_interval,1))+nvl(v_run_interval,1)<=sysdate) then
  return 1;
    else
   return 0;
   end if;
   --依赖日期大于作业日期,则满足依赖条件
     if(v_lean_date is not null and v_job_date is not null and v_lean_date-v_job_date>0) then
     return 1;
     else
     return 0;
     end if;

       exception
  when others then

      dbms_output.put_line(v_rely_sql||sqlerrm);
      return 0;

end;

--获取依赖作业执行情况-->结束

  --表同步->开始
  procedure mv_tab_sync(v_mvlts_id number) is
    v_opt_log_id number;
    v_prg_name varchar(30) := 'MV_LOCAL_TAB_SYNC';
    v_dblog_title varchar(256) := C_LOG_TITLE||'.'||v_prg_name;
    v_step_name varchar(1024);
    v_opt_res clob;
    v_cnt int;
    v_sql varchar2(4000);
    v_sql_clob clob;
    v_sql1 varchar2(4000);
    v_mvlts_row CT_MV_LOCAL_TAB_SYNC%ROWTYPE;
    v_curr_rownum number:=0;
    
    v_end_status number;
    v_job_date varchar2(10);
    v_date_exp varchar2(10);
    v_tmp_tablename varchar2(30);
    v_table_owner  varchar2(30);
  begin
select username into v_table_owner from user_users ;

  v_step_name:='获取同步配置信息';
   VERS_TOOLS.opt_db_begin(v_dblog_title, v_mvlts_id||'.'||v_step_name,null, v_opt_log_id);
    select * into v_mvlts_row from CT_MV_LOCAL_TAB_SYNC where MVLTS_ID=v_mvlts_id;
    v_opt_res:='成功获取配置';
   VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,v_opt_res,0);
   if(v_mvlts_row.lt_dt_col_exp is not null ) then
    v_date_exp:=trim(v_mvlts_row.lt_dt_col_exp);
    else  v_date_exp:='YYYY-MM-DD';
   end if;
   v_job_date :=to_char(v_mvlts_row.last_dat_date,v_date_exp);
    v_step_name:='检查本地表是否已创建';
   VERS_TOOLS.opt_db_begin(v_dblog_title, v_mvlts_id||'.'||v_step_name,v_job_date, v_opt_log_id);
    select VERS_TOOLS.is_usr_tab_exists(upper(v_mvlts_row.lt_tab_ename)) into v_cnt from dual;
    v_opt_res:='本地表检查完毕';
   VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,v_opt_res,0);



    v_step_name:='开始同步数据';
   VERS_TOOLS.opt_db_begin(v_dblog_title, v_mvlts_id||'.'||v_step_name,v_job_date, v_opt_log_id);
    v_opt_res:=v_step_name||',组装查询条件';
   v_sql1:=' where 1=1 ';
   if v_mvlts_row.test_rows is not null then
    v_sql1 := v_sql1||' and rownum<'||to_char(v_mvlts_row.test_rows);
   end if;
     if v_mvlts_row.rt_owhere_exp is not null then
    v_sql1 := v_sql1||' and '||replace(replace(replace(to_char(v_mvlts_row.rt_owhere_exp) ,
                  '$[YYYYMMDD]$', to_char(v_mvlts_row.last_dat_date ,'yyyymmdd'))
                  ,'$[YYYY-MM-DD]$',to_char(v_mvlts_row.last_dat_date, 'yyyy-mm-dd'))
                  ,'$[YYYY/MM/DD]$',to_char(v_mvlts_row.last_dat_date, 'yyyy/mm/dd'));
   end if;
   dbms_output.put_line(v_sql1);
   v_opt_res:=v_opt_res||',检查同步类型';
   case v_mvlts_row.sync_type

   when 1 then --清空，全量插入
   v_opt_res:=v_opt_res||',同步类型:1,插入数据';
  VERS_TOOLS.insert_data_by_trunc(v_mvlts_row.RT_TAB_OWNER,v_mvlts_row.rt_tab_ename,v_mvlts_row.rt_sel_parallel,v_mvlts_row.rt_sel_col,v_sql1,v_table_owner,
  v_mvlts_row.lt_tab_ename,v_mvlts_row.lt_ins_parallel,v_mvlts_row.lt_ins_col,null,4,v_curr_rownum,v_sql);
   v_opt_res := v_opt_res||';影响行数'||to_char(v_curr_rownum)||v_sql;

   when 2 then --删除数据，增量插入
   v_opt_res:=v_opt_res||',同步类型:2,删除数据';
   v_cnt:=0;
    v_curr_rownum:=0;
    v_sql :='select count(1)   from '||v_mvlts_row.LT_TAB_ENAME ||' where '||v_mvlts_row.lt_dt_col||'='''||v_job_date||'''';
  -- dbms_output.put_line(v_sql);
   execute immediate v_sql into v_cnt;
   if(v_cnt>0) then
   v_sql :='delete from '||v_mvlts_row.LT_TAB_ENAME ||' where '||v_mvlts_row.lt_dt_col||'='''||v_job_date||'''';
  -- dbms_output.put_line(v_sql);
   execute immediate v_sql;
    v_curr_rownum:=SQL%ROWCOUNT;
    v_opt_res := v_opt_res||';影响行数'||to_char(v_curr_rownum)||v_sql;
   commit;
   end if;
    v_opt_res :=v_opt_res||',组装插入SQL';
    VERS_TOOLS.get_matched_inser_sql(v_mvlts_row.RT_TAB_OWNER ,v_mvlts_row.rt_tab_ename,v_mvlts_row.rt_sel_parallel,v_mvlts_row.rt_sel_col,v_sql1,v_table_owner,
     v_mvlts_row.lt_tab_ename,v_mvlts_row.lt_ins_parallel,v_mvlts_row.lt_ins_col, v_sql_clob);
     execute immediate v_sql_clob;
      v_curr_rownum := SQL%ROWCOUNT;
     v_opt_res := v_opt_res||';影响行数'||to_char(v_curr_rownum)||v_sql_clob;
    when 3 then --MERGE，暂时只支持同表结构
    v_opt_res:=v_opt_res||',同步类型:3,组装MERGE语句';
    --调用工具包组装merg语句
    vers_tools.get_merge_sql(v_mvlts_row.pk_col,v_mvlts_row.RT_TAB_OWNER ,v_mvlts_row.rt_tab_ename,v_table_owner,
     v_mvlts_row.lt_tab_ename,v_sql1,v_sql_clob);
    execute immediate v_sql_clob;
      v_curr_rownum := SQL%ROWCOUNT;
     v_opt_res := v_opt_res||';影响行数'||to_char(v_curr_rownum)||'sql:'||v_sql_clob;

  /*  when 4 then --先提取增量数据，再MERGE，暂时只支持同表结构
    v_opt_res:=v_opt_res||',同步类型:4,将增量数据放到临时表';
    --生成临时表
    v_tmp_tablename :=vers_tools.get_temp_tablename();
    vers_tools.get_matched_minus_sql(v_tmp_tablename,v_mvlts_row.RT_TAB_OWNER ,v_mvlts_row.rt_tab_ename,v_mvlts_row.rt_sel_parallel,v_mvlts_row.rt_sel_col,v_sql1,v_mvlts_row.lt_ins_parallel,'odsbdata',v_mvlts_row.lt_tab_ename,v_mvlts_row.lt_ins_parallel,v_mvlts_row.lt_dt_col,v_sql);
     execute immediate v_sql;
    --调用工具包组装merg语句
   vers_tools.get_merge_sql(v_mvlts_row.pk_col,'odsbdata' ,v_tmp_tablename,'odsbdata', v_mvlts_row.lt_tab_ename,'',v_sql_clob);
  execute immediate v_sql_clob;
   v_curr_rownum := SQL%ROWCOUNT;
    vers_tools.drop_tab_no_interruption(v_tmp_tablename);
   v_opt_res := v_opt_res||';影响行数'||to_char(v_curr_rownum)||v_sql||v_sql_clob;*/

   end case;
   ----结束同步数据
   --处理表 (备份，表分析，重建失效索引等)
   v_opt_res:=v_opt_res||',表日终';
   VERS_TOOLS.handle_tab_end(v_mvlts_row.lt_tab_ename,to_char(v_mvlts_row.last_dat_date,'yyyymmdd'));
   up_job_end(v_mvlts_id,0,v_curr_rownum,v_end_status);
    VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,v_opt_res,0);
   exception
    when others then
      dbms_output.put_line(sqlerrm);
      up_job_end(v_mvlts_id,1,0,v_end_status);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,substr(v_opt_res||'-调度异常-SQL:'||v_sql||'条件:'||v_sql1||sqlerrm,1,4000),1);

  end mv_tab_sync;
  --物化视图本地表同步->结束

--作业开始，更新数据，参数一： CONF_ID,参数二：返回标识 0 正常 1 异常
  procedure up_job_begin(p_job_id  number,p_up_status out number ) as
   v_opt_log_id  number;
  begin
   update CT_MV_LOCAL_TAB_SYNC t set t.run_begin_time=sysdate,t.run_status=1 where t.mvlts_id=p_job_id;
   if sql%rowcount=0 then
      p_up_status:=1;
    return;
   end if;
   commit;
   p_up_status :=0;
  exception
 when others then
      dbms_output.put_line(sqlerrm);
      VERS_TOOLS.opt_db_begin('MVIEW_MANAGE.up_job_begin', p_job_id||'.'||'作业开始状态更新',null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'调度异常'||sqlerrm,1);
      p_up_status :=1;
  end up_job_begin;


  procedure up_job_end(p_job_id  number,p_flag number,p_curr_num number,p_up_status out number) as
   v_opt_log_id  number;
  begin
  --正常结束
  if (p_flag=0) then
   update CT_MV_LOCAL_TAB_SYNC t set t.run_end_time=sysdate,t.run_status=2,t.run_res='成功，数据正常同步!',t.curr_rowcount=p_curr_num,t.err_retry_time=0  where t.mvlts_id=p_job_id;
   --异常结束
  elsif(p_flag=1) then
   update CT_MV_LOCAL_TAB_SYNC t set t.run_end_time=sysdate,t.run_status=-1,t.err_retry_time=nvl(err_retry_time,0)+1,t.run_res='失败，数据同步异常，具体查看日志表!' where t.mvlts_id=p_job_id;
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
      VERS_TOOLS.opt_db_begin('MVIEW_MANAGE.up_job_end', p_job_id||'.'||'作业结束状态更新',null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'调度异常'||sqlerrm,1);
         p_up_status:=1;
  end up_job_end;


end MVIEW_MANAGE;
