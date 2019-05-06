create or replace package body odsbdata.MVIEW_MANAGE
is


  --ȫ�ֱ���->��ʼ
  C_PAG_NAME varchar(30) := 'MVIEW_MANAGE';
  C_MAIN_VERSION varchar(10) := 'v1.0.0';
  C_LOG_TITLE varchar(64) := C_PAG_NAME||C_MAIN_VERSION;
   --ȫ�ֱ���->����


  --������->��ʼ
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
   v_step_name:='�жϲ�����������ﵽ��󲢷��������� ';
   --��ȡ��ǰ������
   select count(1) into v_ACTIVE_PARALLEL_n from ct_mv_local_tab_sync where RUN_STATUS='1';

    --��ȡ�������
    VERS_TOOLS.get_code_value('MAX_PARALLEL',v_MAX_PARALLEL);
      v_MAX_PARALLEL_n:=to_number(v_MAX_PARALLEL);
     if(v_ACTIVE_PARALLEL_n>=v_MAX_PARALLEL_n) then p_status:=1; return ; end if;
     -----------------------------------------
   --��ʼִ��

    v_step_name:='���ɹ��ĸ���Ϊ��ִ��';
    select count(1) into v_suc_num from ct_mv_local_tab_sync where status='1' and run_status=2 and JOB_PART=p_job_part;
   if(v_suc_num>=1) then

   update ct_mv_local_tab_sync t set  t.last_dat_date=trunc( case when upper(t.dat_date_add)='D' then t.last_dat_date+1 when upper(t.dat_date_add)='M' then add_months(last_day(t.last_dat_date),1)  when upper(t.dat_date_add)='Q' then add_months(trunc(t.last_dat_date,'q'),6)-1 when upper(t.dat_date_add)='Y' then  add_months(to_date(to_char(t.last_dat_date,'yyyy')||'1231','yyyymmdd'),12) when  t.dat_date_add is null and t.run_interval is not null then  nvl(t.RUN_END_TIME,t.last_dat_date)+t.run_interval else sysdate end ,'dd'),t.run_status=0
   where   status='1' and  t.run_status=2 and JOB_PART=p_job_part;
   commit;

   end if;
   v_step_name:='��ȡ���Դ���';
   VERS_TOOLS.get_code_value('ERR_RETRY_TIME',v_ERR_RETRY_TIME);
   vc_ERR_RETRY_TIME:=to_number(nvl(v_ERR_RETRY_TIME,0));
    v_step_name:='��ȡִ����ҵ';

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

   ----��ʼͬ������,����ִ��״̬
   up_job_begin(v_mvlts_id,v_begin_status );
   if(v_begin_status <> 0) then
   return;
   end if;
   
 /*   v_step_name:='���µ�ǰ������';
  v_ACTIVE_PARALLEL:=to_char(v_ACTIVE_PARALLEL_n+1);
   VERS_TOOLS.set_code_value('ACTIVE_PARALLEL',v_ACTIVE_PARALLEL);*/

   --����ͬ������
   VERS_TOOLS.set_code_value('ACTIVE_TIME',to_char(sysdate,'yyyymmdd hh24:mi:ss'));
   mv_tab_sync(v_mvlts_id);
   p_status:=0;
  exception
   when no_data_found then
   p_status:=1;
    return ;
   when others then
      dbms_output.put_line(sqlerrm);
       VERS_TOOLS.opt_db_begin(v_dblog_title, 'ͬ�����س���'||'.'||v_step_name,null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'�����쳣'||sqlerrm,1);
       p_status:=1;
      return ;

  end main_do;
  --������->����


 --��ȡ������ҵִ�����-->��ʼ
function getlean_flg(p_jobid integer) return integer is
pragma autonomous_transaction;
 v_cnt integer;
  v_rely_id integer;
  v_job_date date;--��ҵ����
  v_lean_date date;--��������
  v_rely_sql VARCHAR2(2000);--����SQL
  v_opt_log_id number;
   v_prg_name varchar(30) := 'getlean_flg';
    v_dblog_title varchar(256) := C_LOG_TITLE||'.'||v_prg_name;
    v_run_interval number;
    v_run_end_time date;
  begin
  --�ж��Ƿ���ڴ���ҵ
 -- dbms_output.put_line('jobid:'||p_jobid);
   select count(1) into v_cnt from CT_MV_LOCAL_TAB_SYNC where mvlts_id=p_jobid ;
    if(v_cnt=0) then
      return 0;  --�޴�jobid ֱ�ӷ��� - 0
    end if;
 select a.RELY_ID,a.last_dat_date,a.run_interval,a.run_end_time,b.rely_sql into v_rely_id,v_job_date,v_run_interval,v_run_end_time,v_rely_sql from CT_MV_LOCAL_TAB_SYNC a
 left join CT_SRC_SERV_INFO b on  a.RELY_SYS=b.serv_ename
  where   a.mvlts_id=p_jobid ;
 if(v_rely_id is not null and v_rely_sql is not null ) then
     --SQL ��ȡ������ҵ����
     v_rely_sql:=replace(v_rely_sql,'$[RELY_ID]$',v_rely_id);
     execute immediate v_rely_sql into v_lean_date;
     --��ʱͬ��
  elsif (trim(v_rely_id) is null and nvl(v_run_end_time,v_job_date-nvl(v_run_interval,1))+nvl(v_run_interval,1)<=sysdate) then
  return 1;
    else
   return 0;
   end if;
   --�������ڴ�����ҵ����,��������������
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

--��ȡ������ҵִ�����-->����

  --��ͬ��->��ʼ
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

  v_step_name:='��ȡͬ��������Ϣ';
   VERS_TOOLS.opt_db_begin(v_dblog_title, v_mvlts_id||'.'||v_step_name,null, v_opt_log_id);
    select * into v_mvlts_row from CT_MV_LOCAL_TAB_SYNC where MVLTS_ID=v_mvlts_id;
    v_opt_res:='�ɹ���ȡ����';
   VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,v_opt_res,0);
   if(v_mvlts_row.lt_dt_col_exp is not null ) then
    v_date_exp:=trim(v_mvlts_row.lt_dt_col_exp);
    else  v_date_exp:='YYYY-MM-DD';
   end if;
   v_job_date :=to_char(v_mvlts_row.last_dat_date,v_date_exp);
    v_step_name:='��鱾�ر��Ƿ��Ѵ���';
   VERS_TOOLS.opt_db_begin(v_dblog_title, v_mvlts_id||'.'||v_step_name,v_job_date, v_opt_log_id);
    select VERS_TOOLS.is_usr_tab_exists(upper(v_mvlts_row.lt_tab_ename)) into v_cnt from dual;
    v_opt_res:='���ر������';
   VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,v_opt_res,0);



    v_step_name:='��ʼͬ������';
   VERS_TOOLS.opt_db_begin(v_dblog_title, v_mvlts_id||'.'||v_step_name,v_job_date, v_opt_log_id);
    v_opt_res:=v_step_name||',��װ��ѯ����';
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
   v_opt_res:=v_opt_res||',���ͬ������';
   case v_mvlts_row.sync_type

   when 1 then --��գ�ȫ������
   v_opt_res:=v_opt_res||',ͬ������:1,��������';
  VERS_TOOLS.insert_data_by_trunc(v_mvlts_row.RT_TAB_OWNER,v_mvlts_row.rt_tab_ename,v_mvlts_row.rt_sel_parallel,v_mvlts_row.rt_sel_col,v_sql1,v_table_owner,
  v_mvlts_row.lt_tab_ename,v_mvlts_row.lt_ins_parallel,v_mvlts_row.lt_ins_col,null,4,v_curr_rownum,v_sql);
   v_opt_res := v_opt_res||';Ӱ������'||to_char(v_curr_rownum)||v_sql;

   when 2 then --ɾ�����ݣ���������
   v_opt_res:=v_opt_res||',ͬ������:2,ɾ������';
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
    v_opt_res := v_opt_res||';Ӱ������'||to_char(v_curr_rownum)||v_sql;
   commit;
   end if;
    v_opt_res :=v_opt_res||',��װ����SQL';
    VERS_TOOLS.get_matched_inser_sql(v_mvlts_row.RT_TAB_OWNER ,v_mvlts_row.rt_tab_ename,v_mvlts_row.rt_sel_parallel,v_mvlts_row.rt_sel_col,v_sql1,v_table_owner,
     v_mvlts_row.lt_tab_ename,v_mvlts_row.lt_ins_parallel,v_mvlts_row.lt_ins_col, v_sql_clob);
     execute immediate v_sql_clob;
      v_curr_rownum := SQL%ROWCOUNT;
     v_opt_res := v_opt_res||';Ӱ������'||to_char(v_curr_rownum)||v_sql_clob;
    when 3 then --MERGE����ʱֻ֧��ͬ��ṹ
    v_opt_res:=v_opt_res||',ͬ������:3,��װMERGE���';
    --���ù��߰���װmerg���
    vers_tools.get_merge_sql(v_mvlts_row.pk_col,v_mvlts_row.RT_TAB_OWNER ,v_mvlts_row.rt_tab_ename,v_table_owner,
     v_mvlts_row.lt_tab_ename,v_sql1,v_sql_clob);
    execute immediate v_sql_clob;
      v_curr_rownum := SQL%ROWCOUNT;
     v_opt_res := v_opt_res||';Ӱ������'||to_char(v_curr_rownum)||'sql:'||v_sql_clob;

  /*  when 4 then --����ȡ�������ݣ���MERGE����ʱֻ֧��ͬ��ṹ
    v_opt_res:=v_opt_res||',ͬ������:4,���������ݷŵ���ʱ��';
    --������ʱ��
    v_tmp_tablename :=vers_tools.get_temp_tablename();
    vers_tools.get_matched_minus_sql(v_tmp_tablename,v_mvlts_row.RT_TAB_OWNER ,v_mvlts_row.rt_tab_ename,v_mvlts_row.rt_sel_parallel,v_mvlts_row.rt_sel_col,v_sql1,v_mvlts_row.lt_ins_parallel,'odsbdata',v_mvlts_row.lt_tab_ename,v_mvlts_row.lt_ins_parallel,v_mvlts_row.lt_dt_col,v_sql);
     execute immediate v_sql;
    --���ù��߰���װmerg���
   vers_tools.get_merge_sql(v_mvlts_row.pk_col,'odsbdata' ,v_tmp_tablename,'odsbdata', v_mvlts_row.lt_tab_ename,'',v_sql_clob);
  execute immediate v_sql_clob;
   v_curr_rownum := SQL%ROWCOUNT;
    vers_tools.drop_tab_no_interruption(v_tmp_tablename);
   v_opt_res := v_opt_res||';Ӱ������'||to_char(v_curr_rownum)||v_sql||v_sql_clob;*/

   end case;
   ----����ͬ������
   --����� (���ݣ���������ؽ�ʧЧ������)
   v_opt_res:=v_opt_res||',������';
   VERS_TOOLS.handle_tab_end(v_mvlts_row.lt_tab_ename,to_char(v_mvlts_row.last_dat_date,'yyyymmdd'));
   up_job_end(v_mvlts_id,0,v_curr_rownum,v_end_status);
    VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,v_opt_res,0);
   exception
    when others then
      dbms_output.put_line(sqlerrm);
      up_job_end(v_mvlts_id,1,0,v_end_status);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,substr(v_opt_res||'-�����쳣-SQL:'||v_sql||'����:'||v_sql1||sqlerrm,1,4000),1);

  end mv_tab_sync;
  --�ﻯ��ͼ���ر�ͬ��->����

--��ҵ��ʼ���������ݣ�����һ�� CONF_ID,�����������ر�ʶ 0 ���� 1 �쳣
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
      VERS_TOOLS.opt_db_begin('MVIEW_MANAGE.up_job_begin', p_job_id||'.'||'��ҵ��ʼ״̬����',null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'�����쳣'||sqlerrm,1);
      p_up_status :=1;
  end up_job_begin;


  procedure up_job_end(p_job_id  number,p_flag number,p_curr_num number,p_up_status out number) as
   v_opt_log_id  number;
  begin
  --��������
  if (p_flag=0) then
   update CT_MV_LOCAL_TAB_SYNC t set t.run_end_time=sysdate,t.run_status=2,t.run_res='�ɹ�����������ͬ��!',t.curr_rowcount=p_curr_num,t.err_retry_time=0  where t.mvlts_id=p_job_id;
   --�쳣����
  elsif(p_flag=1) then
   update CT_MV_LOCAL_TAB_SYNC t set t.run_end_time=sysdate,t.run_status=-1,t.err_retry_time=nvl(err_retry_time,0)+1,t.run_res='ʧ�ܣ�����ͬ���쳣������鿴��־��!' where t.mvlts_id=p_job_id;
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
      VERS_TOOLS.opt_db_begin('MVIEW_MANAGE.up_job_end', p_job_id||'.'||'��ҵ����״̬����',null, v_opt_log_id);
       VERS_TOOLS.opt_db_end(v_opt_log_id,0 ,'�����쳣'||sqlerrm,1);
         p_up_status:=1;
  end up_job_end;


end MVIEW_MANAGE;
