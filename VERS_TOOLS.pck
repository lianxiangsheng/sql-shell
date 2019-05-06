create or replace package body odsbdata.VERS_TOOLS is
  /******************************************************************************
   * TITLE      : �����������ݼ��г����-���߰�
   * DESCRIPTION: ���ù��ߣ��������õĹ��ߺ������洢������ӵ�������﹩���ʹ��
   * AUTHOR     : �׻���
   * VERSION    : 1.0
   * DATE       : 2017-06-07
   * MODIFY     : �����޸İ汾�ڴ��г�
               20171205 �׻��� ����MOVE�����
             20180108 �׻��� handle_tab_end �Ż������ڷ����������ͨ��ʽ���� �������������ֶ� �Ż�Lob�ֶ�move����
             20180109 �׻��� move_table  �Ż�������lob�ֶε�Move ֧��
             20181023 �⿵�� handle_tab_end �Ż��ռ�ͳ����Ϣ�߼�,����CT_TAB_PARAM��IS_ANALY_TAB�����ж��Ƿ���Ҫ�ռ�ͳ����Ϣ@������2018R1431
  ******************************************************************************/
  --ȫ�ֱ���->��ʼ
  V_PAG_NAME varchar(30) := 'DWTOOLS';
  V_MAIN_VERSION varchar(10) := 'v1.0.0';
  V_LOG_TITLE varchar(64) := V_PAG_NAME||V_MAIN_VERSION;
  --ȫ�ֱ���->����

  --�����������ӵ�SQL���ʽ->��ʼ
  function get_l_r_conn_sql(col_list varchar2, --�����б�
                             left_tab varchar2, --���
                             right_tab varchar2 --�ұ�
                          ) return varchar2 is
     v_ret varchar2(2048);
     v_col_list varchar2(2048);
     v_col_list_left varchar2(2048);
     v_the_col varchar2(30);
     v_tmp1  varchar2(512);
  begin
     --�������ո�
     v_col_list := col_list;
     v_col_list := ','||trim(v_col_list)||',';
     v_col_list := replace(v_col_list,' ','');

     --�������ұ��ʽ
     v_col_list_left := ',';
     v_ret := '';
     loop
         v_the_col := get_str(v_col_list,v_col_list_left,',');
         exit when v_the_col is null;
         v_col_list_left := v_col_list_left||v_the_col||',';
         v_tmp1 := left_tab||'.'||v_the_col||'='||right_tab||'.'||v_the_col;
         if v_ret is not null then
            v_ret := v_ret||' and ';
         end if;
         v_ret := v_ret||v_tmp1;
     end loop;
     return v_ret;
  exception
     when others then
        return null;
        raise;
  end;
  --�����������ӵ�SQL���ʽ->����

  --�����û��������Ƿ����->��ʼ
  function is_usr_idx_exists(tab_name varchar2, --����
                             idx_name varchar2 --������
                          ) return int is
    v_ret int ;
  begin
    select count(*) into v_ret from user_indexes where table_name = upper(tab_name) and index_name = upper(idx_name);
    return v_ret;
  exception
    when others then
      return -1;
      raise;
  end;
  --�����û��������Ƿ����->����

  --�����û����Ƿ����->��ʼ
  function is_usr_tab_exists(tab_name varchar2 --����
                          ) return int is
    v_ret int ;
  begin
    select count(*) into v_ret from user_tables where table_name = upper(tab_name);
    return v_ret;
  exception
    when others then
      return -1;
      raise;
  end;
  --�����û����Ƿ����->����

  --ɾ��������־->��ʼ
  procedure del_opt_log_inf(keep_days int) is --��������
     v_error varchar(2048);
     v_once_rownum int := 1000;
     v_max_log_id number;
  begin
     select max(LOG_ID) into v_max_log_id from V_OPT_LOG_INF where LOG_TIME < (sysdate-keep_days);
     if v_max_log_id is not null then
        delete from V_OPT_LOG_INF where LOG_ID <= v_max_log_id and rownum < v_once_rownum;
        opt_file_log(V_LOG_TITLE,'ɾ��������־����',to_char(SQL%ROWCOUNT));
     end if;
  exception
    when others then
      v_error := '['|| SQLERRM || ']v_max_log_id['||to_char(v_max_log_id)||']';
      opt_file_errlog(V_LOG_TITLE,'del_opt_log_inf',v_error,1);
      raise;
  end del_opt_log_inf;
  --ɾ��������־->����

  --����SQL���ڱ��ʽ->��ʼ
  function get_sql_date2(date_value date, --����ֵ
                         date_fmt varchar2    --���ڸ�ʽ
                          ) return varchar2 is
    v_ret varchar2(512) ;
  begin
    if date_value is null then
       v_ret := 'null';
    else
       v_ret := 'to_date('''||to_char(date_value,date_fmt)||''','''||date_fmt||''')';
    end if;
    return v_ret;
  exception
    when others then
      raise_application_error (-20105, '��Ч����'||date_value||'����Ч��ʽ'||date_fmt);
      return null;
      raise;
  end;
  --����SQL���ڱ��ʽ->����

  --����SQL���ڱ��ʽ->��ʼ
  function get_sql_date(date_value varchar2, --����ֵ
                        date_fmt varchar2    --���ڸ�ʽ
                          ) return varchar2 is
    v_ret varchar2(512) ;
    v_date_value date;
  begin
    if date_value is null then
       v_ret := 'null';
    else
       v_date_value := to_date(date_value,date_fmt);
       v_ret := 'to_date('''||to_char(v_date_value,date_fmt)||''','''||date_fmt||''')';
    end if;
    return v_ret;
  exception
    when others then
      raise_application_error (-20105, '��Ч����'||date_value||'����Ч��ʽ'||date_fmt);
      return null;
      raise;
  end;
  --����SQL���ڱ��ʽ->����

  --����SQL��ֵ���ʽ->��ʼ
  function get_sql_num(num_value varchar2 --��ֵ
                          ) return varchar2 is
    v_ret varchar2(128) ;
    v_num_value number;
  begin
    if num_value is null then
       v_ret := 'null';
    else
       v_num_value := to_number(num_value);
       v_ret := to_char(v_num_value);
    end if;
    return v_ret;
  exception
    when others then
      raise_application_error (-20104, '��Ч��ֵ'||num_value);
      return null;
      raise;
  end;

  --����SQL�ַ������ʽ->��ʼ
  function get_sql_str(str_value varchar2 --�ַ���ֵ
                          ) return varchar2 is
    v_ret varchar2(16384) ;
  begin
    if str_value is null then
       v_ret := null;
    else
       v_ret := str_value;
       if instr(v_ret,'''') > 0 then
          v_ret := replace(v_ret,'''','''''');
       end if;
       v_ret := ''''||v_ret||'''';
    end if;
    return v_ret;
  exception
    when others then
      return null;
      raise;
  end;
  --����SQL�ַ������ʽ->����

  --�������ڲ�->��ʼ
  function date_diff(datepart varchar2, --���ڲ���(����MS-SQL SERVER)
                   startdate  date, --��ʼ����
                   enddate  date --��������
                          ) return int is
    v_ret number ;
    v_error varchar2(2048);
    ERR_DATE_PART exception;
  begin
    v_ret := to_number(enddate-startdate);
    case datepart
      when 'yy' then      --��
         v_ret := v_ret/365;
      when 'qq' then      --��
         v_ret := v_ret/365/4;
      when 'mm' then      --��
         v_ret := v_ret/12;
      when 'dd' then      --��
         null;
      when 'wk' then      --��
         v_ret := v_ret/7;
      when 'hh' then      --ʱ
         v_ret := v_ret*24;
      when 'mi' then      --��
         v_ret := v_ret*24*60;
      when 'ss' then      --��
         v_ret := v_ret*24*60*60;
      else
        raise ERR_DATE_PART;
    end case;
    return v_ret;
  exception
    when ERR_DATE_PART then
      v_error := '��Ч���ڲ���'||datepart;
      raise_application_error (-20103, v_error);
    when others then
      return null;
      raise;
  end;
  --�������ڲ�->����

  --��ȡ�ַ������м䲿��->��ʼ
  function get_str(src_str varchar2, --Դ�ַ���
                   begin_str varchar2, --��ʼ��
                   end_str  varchar2 --������
                          ) return varchar2 is
    v_ret_str varchar2(16384) ;
    v_begin_len number := length(begin_str);
    v_begin number;
    v_end_len number;
    v_end number;
    v_cut number;
    v_src_str1 varchar2(16384) ;
  begin
    v_begin := instr(src_str,begin_str);
    v_end_len := length(end_str);
    if v_end_len = 0 then
       v_end_len := length(end_str)+1;

    else
       v_Cut := v_begin+v_begin_len+1;
       v_src_str1 := substr(src_str,v_cut);
       v_end := instr(v_src_str1,end_str)+v_cut-1;
    end if;
    v_ret_str := substr(src_str,v_begin+v_begin_len,v_end-v_begin-v_begin_len);
    return v_ret_str;
  exception
    when others then
      return '';
  end;
  --��ȡ�ַ������м䲿��->����

  --��ȡһ������ֵ->��ʼ
  procedure get_code_value(CD_ENAME varchar2, cd_value out varchar2) as
     v_error varchar(2048);
     v_sql varchar(2048);
  begin
     v_sql := 'select CD_VALUE from CT_MAIN_CODE_INFO where CD_ENAME = :1 ';
     execute immediate v_sql into cd_value using CD_ENAME;
  exception
    when NO_DATA_FOUND then
      v_error := '����['||CD_ENAME||']������';
      opt_file_errlog(V_LOG_TITLE,'get_code_value',v_error,1);
      raise_application_error (-20102, v_error);
    when others then
      v_error := '['||v_sql||'][' || SQLERRM || ']';
      opt_file_errlog(V_LOG_TITLE,'get_code_value',v_error,1);
      raise;
  end get_code_value;
  --��ȡһ������ֵ->����
  --------------------------------------------------------------------

 --����һ������ֵ->��ʼ
  procedure set_code_value(CD_ENAME varchar2, CD_VALUE  varchar2) as
     v_error varchar(2048);
     v_sql varchar(2048);
   v_tmp varchar(2048);
  begin
     v_sql := 'select CD_VALUE from ct_main_code_info where CD_ENAME = :1';
     execute immediate v_sql into v_tmp using CD_ENAME;
   v_sql := 'update CT_MAIN_CODE_INFO set CD_VALUE = :1 where CD_ENAME = :2 ';
   execute immediate v_sql using CD_VALUE,  CD_ENAME ;
   commit;
  exception
    when NO_DATA_FOUND then
      v_error := '����['||CD_ENAME||']������';
      opt_file_errlog(V_LOG_TITLE,'set_code_value',v_error,1);
      rollback;
      raise_application_error (-20102, v_error);
    when others then
      v_error := '['||v_sql||'][' || SQLERRM || ']';
      opt_file_errlog(V_LOG_TITLE,'set_code_value',v_error,1);
      rollback;
      raise;
  end set_code_value;
  --����һ������ֵ->����

  --������־����ӡ���ļ�->��ʼ
  procedure opt_file_log(file_title varchar2,log_title varchar2, log_inf clob) as
    v_log_level  varchar(10) := '��Ϣ';
    v_file_title  varchar(100);
    v_log_return number := 0;

  begin
    v_file_title  := upper(file_title) || '.' || to_char(sysdate, 'yyyymmdd');
    v_log_return := write_log_file(v_log_level, log_title , v_file_title, log_inf);

  end opt_file_log;
  --������־����ӡ���ļ�->����
  --------------------------------------------------------------------
  --����������־����ӡ���ļ�->��ʼ
  procedure opt_file_errlog(file_title varchar2,log_title varchar2, errlog_inf clob, is_errlog_only integer) as
    v_log_level  varchar(10) := '����';
    v_file_title  varchar(100);
    v_file_title_err  varchar(100);
    v_log_return number := 0;
  begin
    v_file_title_err  := upper(file_title) || '.ERR.' || to_char(sysdate, 'yyyymmdd');
    v_log_return := write_log_file(v_log_level, log_title , v_file_title_err, errlog_inf);
    if is_errlog_only!=1 then
      v_file_title  := upper(file_title) || '.' || to_char(sysdate, 'yyyymmdd');
      v_log_return := write_log_file(v_log_level, log_title , v_file_title, errlog_inf);
    end if;

  end opt_file_errlog;
  --����������־����ӡ���ļ�->����
  --------------------------------------------------------------------
  --��ʼһ��������־->��ʼ
  procedure opt_db_begin(opt_title  varchar2, --��������
                         opt_desc   varchar2, --��������
                         etl_dt    varchar2,--��������
                         ret_log_id out number   --������־��ʶ
                       ) as
    v_error varchar(4000);
  begin
    select SEQ_OPT_LOG_INF.NEXTVAL into ret_log_id from dual ;
    insert into V_OPT_LOG_INF(LOG_ID,LOG_TIME,ETL_DT,OPT_TITLE,OPT_DESC,SESSIONID,START_TIME)
    values(ret_log_id,sysdate,etl_dt,opt_title,opt_desc,userenv('SESSIONID'),CURRENT_TIMESTAMP);
    commit;
  exception
    when others then
      v_error := '[' || SQLCODE || '][' || SQLERRM || ']';
      opt_file_errlog(V_LOG_TITLE,'opt_db_begin',v_error,1);
      --rollback;
      raise;
  end;
  --��ӡ��Ϣ���ļ�->����
  --------------------------------------------------------------------
  --����һ��������־->��ʼ
  procedure opt_db_end(opt_log_id number, --��־�ı�ʶ
                       row_count number,--Ӱ������
                       opt_res   clob, --�������
                       status number --״̬ 0���� 1 �쳣
                       ) as
    err_opt_log_id_not_found exception;
    v_error varchar(4000);
    v_cnt int;
    v_row_count number;
    v_opt_res clob;
    v_exe_status  number;
     V_MISECOND      INTEGER;                 --��ʱ������
     V_SECONDS       INTEGER;                 --��ʱ����
     V_MINUTES       INTEGER;                 --��ʱ����
     V_HOURS         INTEGER;                 --��ʱ��ʱ
     V_DAYS          INTEGER;                 --��ʱ����
     V_EXEC_TIME_STR VARCHAR2(30);            --ִ��ʱ��
     V_START_TIME     TIMESTAMP;               --�ϴ�����ʱ��
     V_CURRENT_TM  TIMESTAMP;
      V_EXEC_TIME_DES VARCHAR2(30);            --�����ʱ������
     V_EXEC_TIME_S   NUMBER(30, 6);           --�����ʱ��ʱ�������
  begin
    select count(*),min(START_TIME) into v_cnt,V_START_TIME from V_OPT_LOG_INF where log_id=opt_log_id;
    if v_cnt=0 then
       raise err_opt_log_id_not_found;
    end if;
    v_opt_res := opt_res;
    v_row_count:=row_count;
    v_exe_status:=status;
    V_CURRENT_TM:= CURRENT_TIMESTAMP;
    --��ȡִ��ʱ��
     SELECT
       TO_CHAR(V_CURRENT_TM - V_START_TIME) INTO V_EXEC_TIME_STR
     FROM DUAL;

     V_MISECOND := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, INSTR(V_EXEC_TIME_STR,' ')+10,3));  -- ����
     V_SECONDS  := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, INSTR(V_EXEC_TIME_STR,' ')+7,2));   -- ��
     V_MINUTES  := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, INSTR(V_EXEC_TIME_STR,' ')+4,2));   -- ��
     V_HOURS    := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, INSTR(V_EXEC_TIME_STR,' ')+1,2));   -- ʱ
     V_DAYS     := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, 1, INSTR(V_EXEC_TIME_STR,' ')));    -- ��

     V_EXEC_TIME_DES :=V_DAYS||'��'||V_HOURS||'ʱ'||V_MINUTES||'��'||V_SECONDS||'��'||V_MISECOND||'����';

     V_EXEC_TIME_S := V_DAYS * 86400 + V_HOURS * 3600 + V_MINUTES * 60 + V_SECONDS + V_MISECOND * 0.001;
    update V_OPT_LOG_INF
    set OPT_RES=v_opt_res,
        ROW_COUNT=v_row_count,
        END_TIME=V_CURRENT_TM,
        EXE_TIME=V_EXEC_TIME_DES,
        EXE_TIME_S=V_EXEC_TIME_S,
        exe_status=v_exe_status
    where LOG_ID=opt_log_id;
    commit;
  exception
    when err_opt_log_id_not_found then
      v_error := '��־��ʶ������';
      opt_file_errlog('DWTOOLS','opt_db_end',v_error,1);
      raise_application_error (-20101, v_error);
    when others then
      v_error := '[' || SQLCODE || '][' || SQLERRM || ']';
      opt_file_errlog(V_LOG_TITLE,'opt_db_end',v_error,1);
      rollback;
      raise;
  end;

  --------------------------------------------------------------------

  --------------------------------------------------------------------
  --��ӡ��Ϣ���ļ�->��ʼ
  function write_log_file(log_level varchar2, --��־����
                          log_title  varchar2, --��־����
                          file_title varchar2, --�ļ���
                          log_inf   clob --��־����
                          ) return number is
    fp UTL_FILE.file_type;
  begin
    fp := UTL_FILE.fopen('LOG_DIR', file_title, 'a', 16384 );

    /* �����ʽΪ[�Ự][ʱ��][����][����][����] */
    UTL_FILE.putf(fp,
                  '[%s][%s][%s][%s][%s]\n',
                  userenv('SESSIONID'),
                  to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),
                  log_level,
                  log_title,
                  log_inf);

    UTL_FILE.fclose(fp);

    return 0;
  exception
    when utl_file.invalid_path then

      return 1;
    when others then
      -- consider logging the error and then re-raise
      return 1;

  end;
  --��ӡ��Ϣ���ļ�->����


  --����MD5���ܽ��->��ʼ
  function get_md5(src_str varchar2 --Դ��
                          ) return varchar2 is
  begin
     RETURN Utl_Raw.Cast_To_Raw(DBMS_OBFUSCATION_TOOLKIT.MD5(input_string => src_str));
  exception
     when others then
        return null;
  end;
  --����MD5���ܽ��->����


  FUNCTION get_enc_val(p_in_val IN VARCHAR2,p_key VARCHAR2,p_iv VARCHAR2)
  RETURN VARCHAR2 IS
  l_enc_val VARCHAR2(200);
  l_in_val  VARCHAR2(200);
  l_iv      VARCHAR2(200);
BEGIN
  l_in_val := RPAD(p_in_val, (8 * ROUND(LENGTH(p_in_val) / 8, 0) + 8));
  l_iv      := RPAD(p_iv, (8 * ROUND(LENGTH(p_iv) / 8, 0) + 8));
  l_enc_val := DBMS_OBFUSCATION_TOOLKIT.des3encrypt(input_string => l_in_val,
                                                    key_string   => p_key,
                                                    iv_string    => l_iv);
  l_enc_val := RAWTOHEX(UTL_RAW.cast_to_raw(l_enc_val));
  RETURN l_enc_val;
  end get_enc_val;

  --���ؽ����ַ��������ڽ���
  FUNCTION get_dec_val(p_in_val IN VARCHAR2,p_key VARCHAR2,p_iv VARCHAR2)
  RETURN VARCHAR2 IS
  l_dec_val VARCHAR2(200);
  l_in_val  VARCHAR2(200);
  l_iv      VARCHAR2(200);
BEGIN
  l_in_val  := p_in_val;
  l_iv      := RPAD(p_iv, (8 * ROUND(LENGTH(p_iv) / 8, 0) + 8));
  l_dec_val:=dbms_obfuscation_toolkit.des3decrypt(
  utl_raw.cast_to_varchar2(l_in_val),key_string=>p_key,iv_string    => l_iv);
  RETURN trim(l_dec_val);
  end get_dec_val;

  --���ؽ��ܴ�->����
procedure copy_indexes(p_orig_table varchar2,p_tar_table varchar2,p_parallel varchar2,p_tablespace varchar2) is
    v_idx_count integer;
    v_idx_ddl   varchar2(1000);
    v_idx_name  varchar2(100);
    v_first_part  varchar2(1000);
    v_last_part   varchar2(1000);
    v_orig_table varchar2(100);
    v_tar_table varchar2(100);
    v_parallel varchar2(5);
    v_tablespace varchar2(30);
begin
 v_orig_table:=upper(p_orig_table);
  v_tar_table:=upper(p_tar_table);
  v_parallel :=p_parallel;
  v_tablespace:=p_tablespace;
  select count(1) into v_idx_count from user_indexes where table_name=upper(v_orig_table);
  --���Դ���û��������ֱ�ӷ���
  if v_idx_count = 0 then
    return;
  end if;
  for item in (select rownum rn,table_owner,index_name,table_name,tablespace_name,partitioned,regexp_substr(to_char(substr(dbms_metadata.get_ddl('INDEX',a.index_name),1,4000)),'CREATE(.)*') idx_cmd from user_indexes a where table_name=upper(v_orig_table) AND INDEX_TYPE<>'LOB') loop
    v_idx_name:=substr('idx'||item.rn||'_'||v_tar_table,1,30);

    --���м�" ON "��DDL����жϳ�������
    v_first_part:=substr(item.idx_cmd,1,instr(item.idx_cmd,' ON '));
    v_last_part:=substr(item.idx_cmd,instr(item.idx_cmd,' ON '),1000);

    --��һ����,��Ŀ�������������滻Դ�����������
    --�ڶ�����,��Ŀ��������滻Դ������
    v_first_part:= regexp_replace(srcstr => v_first_part,pattern => '"'||item.index_name||'"',replacestr => v_idx_name);
    v_last_part:= regexp_replace(srcstr => v_last_part,pattern => '"'||item.table_name||'"',replacestr => v_tar_table);

    --�滻�����ͱ��owner
    v_first_part:=regexp_replace(srcstr => v_first_part,pattern => '"(.)+"\.',replacestr => user||'.');
    v_last_part:=regexp_replace(srcstr => v_last_part,pattern => '"'||item.table_owner||'"',replacestr => user);
    if(item.partitioned='YES') then
    select v_first_part||v_last_part||decode(v_tablespace,null,' tablespace '||item.tablespace_name,' tablespace '||v_tablespace)||' local '||decode(v_parallel,null,' parallel 4 ',' parallel '||v_parallel)||' online nologging' into v_idx_ddl from dual;
    else
    select v_first_part||v_last_part||decode(v_tablespace,null,' tablespace '||item.tablespace_name,' tablespace '||v_tablespace)||decode(v_parallel,null,' parallel 4 ',' parallel '||v_parallel)||' online nologging' into v_idx_ddl from dual;
    end if;
    execute immediate v_idx_ddl;
  end loop;
end copy_indexes;

--����ĳ����ı�ṹ��������һ����
 PROCEDURE copy_table(p_orig_table VARCHAR2, --�ο���
                         p_tar_table  VARCHAR2 --Ŀ���
                         )is
    v_tab_ddl   clob;
    v_orig_table varchar2(100);
    v_tar_table  varchar2(100);
    v_orig_part  varchar2(1000);
    v_tar_part   varchar2(1000);
    v_stat_num integer;
begin
v_orig_table:=upper(p_orig_table);
v_tar_table:=upper(p_tar_table);
   select is_usr_tab_exists(v_orig_table) into v_stat_num from dual;
  if (v_stat_num <=0) then
  return;
  end if;
 --�رձ�����������ȹ���

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS', FALSE);

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'REF_CONSTRAINTS', FALSE);

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS_AS_ALTER', FALSE);
--�رմ洢����ռ�����

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'STORAGE', FALSE);

-- DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'TABLESPACE', FALSE);

--�رմ������PCTFREE��NOCOMPRESS������

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', FALSE);


select regexp_substr(dbms_metadata.get_ddl('TABLE',v_orig_table),'CREATE(.)*') ,dbms_metadata.get_ddl('TABLE',v_orig_table)
 into v_orig_part,v_tab_ddl  from dual;

   --��Ŀ��������滻Դ������
    v_tar_part:= regexp_replace(srcstr => v_orig_part,pattern => '"'||v_orig_table||'"',replacestr => v_tar_table);
   -- dbms_output.put_line(v_tar_part);
    --���滻�õı��滻�������
    v_tab_ddl:= regexp_replace(srcstr => v_tab_ddl,pattern => v_orig_part,replacestr => v_tar_part);

    execute immediate v_tab_ddl;

end copy_table;


PROCEDURE get_tab_constraint_cmd(p_owner VARCHAR2,
                                   p_tab   VARCHAR2,
                                   cmd1    OUT VARCHAR,
                                   cmd2    OUT VARCHAR2,
                                   cmd3    OUT VARCHAR2,
                                   cmd4    OUT VARCHAR2) IS
    v_constraint_cnt NUMBER;
  BEGIN
    SELECT COUNT(1)
      INTO v_constraint_cnt
      FROM all_constraints
     WHERE owner = upper(p_owner) AND table_name = upper(p_tab);

    IF v_constraint_cnt = 0
    THEN
      RETURN;
    END IF;

    SELECT nvl2(cmd1, 'alter table ' || p_owner || '.' || p_tab || ' add ' || cmd1, NULL),
           nvl2(cmd2, 'alter table ' || p_owner || '.' || p_tab || ' add ' || cmd2, NULL),
           nvl2(cmd3, 'alter table ' || p_owner || '.' || p_tab || ' add ' || cmd3, NULL),
           nvl2(cmd4, 'alter table ' || p_owner || '.' || p_tab || ' add ' || cmd4, NULL)
      INTO cmd1, cmd2, cmd3, cmd4
      FROM (SELECT regexp_substr(to_char(dbms_metadata.get_ddl('TABLE', upper(p_tab))), 'CONSTRAINT(.)*', 1, 1, NULL) cmd1,
                   regexp_substr(to_char(dbms_metadata.get_ddl('TABLE', upper(p_tab))), 'CONSTRAINT(.)*', 1, 2, NULL) cmd2,
                   regexp_substr(to_char(dbms_metadata.get_ddl('TABLE', upper(p_tab))), 'CONSTRAINT(.)*', 1, 3, NULL) cmd3,
                   regexp_substr(to_char(dbms_metadata.get_ddl('TABLE', upper(p_tab))), 'CONSTRAINT(.)*', 1, 4, NULL) cmd4
              FROM dual);
  END get_tab_constraint_cmd;

  PROCEDURE get_tab_index_cmd(p_owner      VARCHAR2,
                              p_tab        VARCHAR2,
                              p_parallel   INTEGER,
                              p_tablespace VARCHAR2,
                              cmd1         OUT VARCHAR,
                              cmd2         OUT VARCHAR2,
                              cmd3         OUT VARCHAR2,
                              cmd4         OUT VARCHAR2,
                              cmd5         OUT VARCHAR) IS
    v_idx_cnt NUMBER;
    CURSOR cur_idx IS
      SELECT rownum rn,
             regexp_substr( to_char(SUBSTR(dbms_metadata.get_ddl('INDEX', a.index_name),1,4000)), 'CREATE(.)*') ||
             ' tablespace ' || nvl(p_tablespace, tablespace_name) ||
             ' parallel ' || nvl(p_parallel, 4) idx_cmd
        FROM all_indexes a
       WHERE owner = upper(p_owner) AND table_name = upper(p_tab)
      /* AND    NOT EXISTS
      (SELECT 1
               FROM all_constraints b
              WHERE a.owner = b.owner AND a.index_name = b.constraint_name AND
                    a.table_name = b.table_name)*/
      ;
  BEGIN
    SELECT COUNT(1)
      INTO v_idx_cnt
      FROM all_indexes
     WHERE owner = upper(p_owner) AND table_name = upper(p_tab);
    IF v_idx_cnt = 0
    THEN
      RETURN;
    END IF;

    FOR item IN cur_idx LOOP
      IF item.rn = 1
      THEN
        cmd1 := item.idx_cmd;
      END IF;
      IF item.rn = 2
      THEN
        cmd2 := item.idx_cmd;
      END IF;
      IF item.rn = 3
      THEN
        cmd3 := item.idx_cmd;
      END IF;
      IF item.rn = 4
      THEN
        cmd4 := item.idx_cmd;
      END IF;
      IF item.rn = 5
      THEN
        cmd5 := item.idx_cmd;
      END IF;
    END LOOP;
  END get_tab_index_cmd;

  PROCEDURE drop_all_cons_idx(p_owner VARCHAR2, p_tab VARCHAR2) IS
  BEGIN
    --ɾ��Լ��
    FOR item IN (SELECT constraint_name
                   FROM all_constraints
                  WHERE owner = upper(p_owner) AND table_name = upper(p_tab)) LOOP
      EXECUTE IMMEDIATE 'alter table ' || p_owner || '.' || p_tab ||
                        ' drop constraint ' || item.constraint_name ||
                        ' cascade';
    END LOOP;
    --ɾ������
    FOR item IN (SELECT index_name
                   FROM all_indexes
                  WHERE owner = upper(p_owner) AND table_name = upper(p_tab)) LOOP
      EXECUTE IMMEDIATE 'drop index ' || p_owner || '.' || item.index_name;
    END LOOP;
  END drop_all_cons_idx;

---��ȡ������䣬����Դ���û���Դ����ѯ����������ѯ�ֶΣ���ѯ������Ŀ����û���Ŀ������벢�����������ֶΣ�����SQL
  PROCEDURE get_matched_inser_sql (p_src_owner VARCHAR2,
                                  p_src_tab   VARCHAR2,
                                  p_sel_pal   integer,
                                  p_sel_cols  VARCHAR2,
                                  p_sel_where VARCHAR2,
                                  p_tar_owner VARCHAR2,
                                  p_tar_tab   VARCHAR2,
                                  p_ins_pal   integer,
                                  p_ins_cols  VARCHAR2,
                                  v_sql       OUT clob)
   IS
    TYPE type_col IS RECORD(
      t_col VARCHAR2(100),
      s_col VARCHAR2(100));
    TYPE type_col_tab IS TABLE OF type_col;
    t_col_table   type_col_tab;
    v_src_link    VARCHAR2(100);
    v_tar_link    VARCHAR2(100);
    v_src_tab     VARCHAR2(100);
    v_tar_tab     VARCHAR2(100);
    v_part_insert VARCHAR2(4000);
    v_part_select VARCHAR2(4000);
    v_exe_sql     VARCHAR2(4000);
    v_sel_pal     integer;
    v_ins_pal     integer;
  BEGIN
    v_src_tab := p_src_tab;
    v_tar_tab := p_tar_tab;
    v_sel_pal :=nvl(p_sel_pal,2);
    v_ins_pal :=nvl(p_ins_pal,2);
    IF instr(p_src_tab, '@') > 0
    THEN
      v_src_tab  := substr(p_src_tab, 1, instr(p_src_tab, '@') - 1);
      v_src_link := substr(p_src_tab, instr(p_src_tab, '@'));
    END IF;
    IF instr(p_tar_tab, '@') > 0
    THEN
      v_tar_tab  := substr(p_tar_tab, 1, instr(p_tar_tab, '@') - 1);
      v_tar_link := substr(p_tar_tab, instr(p_tar_tab, '@'));
    END IF;
    --���δ�����ѯ�ֶκͲ����ֶΣ�����Ϊ��ͬ�ṹ���Զ����ɲ������
    if (trim(p_sel_cols) is null or trim(p_ins_cols) is null) then
    v_exe_sql := 'SELECT
       t.column_name t_col,
       s.column_name s_col
        FROM (SELECT owner,
                     table_name,
                     column_name,
                     data_type,
                     data_length,
                     column_id
                FROM all_tab_columns' || v_tar_link || '
               WHERE table_name = ''' || upper(v_tar_tab) ||
                 ''' AND owner = ''' || upper(p_tar_owner) ||
                 ''') t,
             (SELECT owner, table_name, column_name, data_type, data_length
                FROM all_tab_columns' || v_src_link || '
               WHERE table_name =  ''' || upper(v_src_tab) ||
                 ''' AND owner = ''' || upper(p_src_owner) ||
                 ''') s
       WHERE t.column_name = s.column_name AND t.data_type = s.data_type
       ORDER BY t.column_id';

    EXECUTE IMMEDIATE v_exe_sql BULK COLLECT INTO t_col_table;
    -- dbms_output.put_line( 'count:'||t_col_table.count);
   --     dbms_output.put_line( 'f:'||t_col_table.first);
   --        dbms_output.put_line( 'l:'||t_col_table.last);
    FOR item IN 1 .. t_col_table.count LOOP
      /* dbms_output.put_line('item:'||item);
       dbms_output.put_line('t_col:'||t_col_table(item).t_col );
       dbms_output.put_line('s_col:'||t_col_table(item).t_col );*/
      v_part_insert := v_part_insert || t_col_table(item).t_col || ',';
      v_part_select := v_part_select || t_col_table(item).s_col || ',';
     /* dbms_output.put_line('v_part_insert:'||v_part_insert );
      dbms_output.put_line('v_part_select:'||v_part_select );*/
    END LOOP;
    -- dbms_output.put_line('v_part_insert:'||v_part_insert);
   --   dbms_output.put_line('v_part_select:'||v_part_select);
    v_sql := 'insert /*+append,parallel('||v_ins_pal||') */ into ' || p_tar_owner||'.'||p_tar_tab || '(' ||
             rtrim(v_part_insert, ',') || ') select /*+parallel('||v_sel_pal||')*/ ' ||
             rtrim(v_part_select, ',') || ' from ' ||p_src_owner||'.'||p_src_tab||'  '||p_sel_where;
  -- dbms_output.put_line('v_sql1:'||v_sql);
   elsif (trim(p_sel_cols) is not null and trim(p_ins_cols) is not null) then
   v_sql := 'insert /*+append,parallel('||v_ins_pal||') */ into ' || p_tar_owner||'.'||p_tar_tab || '(' ||
             rtrim(p_ins_cols, ',') || ') select /*+parallel('||v_sel_pal||')*/ ' ||
             rtrim(p_sel_cols, ',') || ' from ' ||p_src_owner||'.'||p_src_tab||'  '||p_sel_where;
 -- dbms_output.put_line('v_sql2:'||v_sql);
   end if;
  END;

  PROCEDURE insert_data_by_trunc(p_src_owner   VARCHAR2,
                                   p_src_tab     VARCHAR2,
                                    p_sel_pal   integer,
                                   p_sel_cols  VARCHAR2,--��ѯ�ֶ�
                                    p_sel_where VARCHAR2,
                                   p_tar_owner   VARCHAR2,
                                   p_tar_tab     VARCHAR2,
                                    p_ins_pal   integer,
                                    p_ins_cols  VARCHAR2,--�����ֶ�
                                   p_tar_idx_tbs VARCHAR2,
                                   p_parallel    INTEGER,
                                   p_row_num  OUT integer,
                                   p_v_sql OUT varchar2) IS

    cons_sql1  VARCHAR2(1000);
    cons_sql2  VARCHAR2(1000);
    cons_sql3  VARCHAR2(1000);
    cons_sql4  VARCHAR2(1000);
    idx_sql1   VARCHAR2(1000);
    idx_sql2   VARCHAR2(1000);
    idx_sql3   VARCHAR2(1000);
    idx_sql4   VARCHAR2(1000);
    idx_sql5   VARCHAR2(1000);
    insert_sql VARCHAR2(4000);
    v_partition VARCHAR2(100);
    v_tar_tab   VARCHAR2(100);
  BEGIN
    v_tar_tab:=p_tar_tab;
    --���Ŀ����Ǵ�������,��'DW_CUST_ACCT_INFO_PER partition(part_ccbs_sa)'���ȡ'partition(part_ccbs_sa)'
   v_partition:=regexp_substr(upper(p_tar_tab),'PARTITION(.)+');

   --���Ŀ����Ǵ�������,��'DW_CUST_ACCT_INFO_PER partition(part_ccbs_sa)'���ȡ'DW_CUST_ACCT_INFO_PER'
   if instr(p_tar_tab,' ')>0 then
     v_tar_tab:=substr(p_tar_tab,1,instr(p_tar_tab,' ')-1);
   end if;
    --��ȡԼ��sql
    VERS_TOOLS.get_tab_constraint_cmd(upper(p_tar_owner), upper(v_tar_tab), cons_sql1, cons_sql2, cons_sql3, cons_sql4);
    --��ȡ����sql
    VERS_TOOLS.get_tab_index_cmd(upper(p_tar_owner), upper(v_tar_tab), p_parallel, p_tar_idx_tbs, idx_sql1, idx_sql2, idx_sql3, idx_sql4, idx_sql5);
    --��ȡ��������sql
    VERS_TOOLS.get_matched_inser_sql(upper(p_src_owner), upper(p_src_tab),p_sel_pal,p_sel_cols,p_sel_where, upper(p_tar_owner), upper(v_tar_tab),p_ins_pal,p_ins_cols, insert_sql);

    --dbms_output.put_line(cons_sql1);
    --dbms_output.put_line(cons_sql2);
    --dbms_output.put_line(cons_sql3);
    --dbms_output.put_line(cons_sql4);

    --dbms_output.put_line(idx_sql1);
    --dbms_output.put_line(idx_sql2);
    --dbms_output.put_line(idx_sql3);
    --dbms_output.put_line(idx_sql4);
    --dbms_output.put_line(idx_sql5);

    --ɾ������Լ��������(�������ʱ�����������)
    if trim(v_partition) is null then
       VERS_TOOLS.drop_all_cons_idx(upper(p_tar_owner), upper(v_tar_tab));
    end if;
    --��ձ�
    if trim(v_partition) is not null then
      EXECUTE IMMEDIATE 'alter table '||p_tar_owner||'.'||v_tar_tab||' truncate '||v_partition;
    else
      EXECUTE IMMEDIATE 'truncate table ' || p_tar_owner || '.' || p_tar_tab;
    end if;
    p_v_sql:=insert_sql;
    --��ʼ��������
    EXECUTE IMMEDIATE insert_sql;
    p_row_num:= SQL%ROWCOUNT;
    COMMIT;

    --��������
    if trim(v_partition) is null  then
      IF idx_sql1 IS NOT NULL
      THEN
        EXECUTE IMMEDIATE idx_sql1;
      END IF;
      IF idx_sql2 IS NOT NULL
      THEN
        EXECUTE IMMEDIATE idx_sql2;
      END IF;
      IF idx_sql3 IS NOT NULL
      THEN
        EXECUTE IMMEDIATE idx_sql3;
      END IF;
      IF idx_sql4 IS NOT NULL
      THEN
        EXECUTE IMMEDIATE idx_sql4;
      END IF;
      IF idx_sql5 IS NOT NULL
      THEN
        EXECUTE IMMEDIATE idx_sql5;
      END IF;

      --����Լ��
      IF cons_sql1 IS NOT NULL
      THEN
        EXECUTE IMMEDIATE cons_sql1;
      END IF;
      IF cons_sql2 IS NOT NULL
      THEN
        EXECUTE IMMEDIATE cons_sql2;
      END IF;
      IF cons_sql3 IS NOT NULL
      THEN
        EXECUTE IMMEDIATE cons_sql3;
      END IF;
      IF cons_sql4 IS NOT NULL
      THEN
        EXECUTE IMMEDIATE cons_sql4;
      END IF;
    end if;
    dbms_stats.gather_table_stats(ownname => p_tar_owner,tabname => v_tar_tab,estimate_percent => 5,degree => 4,cascade => true);
  END insert_data_by_trunc;

  FUNCTION get_temp_tablename return varchar2 is
  begin
    return 'tmp_'||userenv('sid')||'_'||to_char(systimestamp,'yyyymmddhh24missff6');
    dbms_lock.sleep(1/1000);
  end get_temp_tablename;

  PROCEDURE drop_tab_no_interruption(v_tabname varchar2) is
  begin
    execute immediate 'drop table '||v_tabname||' purge';
    exception when others then null;
  end;

  --�����ַ������ض��ַ��ָ������
  FUNCTION split(src_string VARCHAR2, split_char VARCHAR2)
    RETURN typ_varchar2 IS
    v_position INTEGER;
    v_str      VARCHAR2(4000);
    v_ret      typ_varchar2;
    i          INTEGER := 1;
  BEGIN
    v_str      := src_string;
    v_ret      := typ_varchar2();
    v_position := instr(v_str, split_char);
    WHILE (v_position > 0) LOOP
      v_ret.extend;
      v_ret(i) := substr(v_str, 1, v_position - 1);
      i := i + 1;
      v_str := substr(v_str, v_position + 1);
      v_position := instr(v_str, split_char);
    END LOOP;
    v_ret.extend;
    v_ret(i) := v_str;
    RETURN v_ret;
  END;
--���ַ������鰴�շָ���תΪһ��
  FUNCTION col_t_row(typ_string typ_varchar2, split_char VARCHAR2)
    RETURN varchar2 IS
    v_str      VARCHAR2(4000);
    v_col_typ  typ_varchar2;
  BEGIN
  v_col_typ:=typ_string;
     FOR item IN v_col_typ.first .. v_col_typ.last LOOP
      v_str := CASE
                 WHEN v_str IS NOT NULL THEN
                  v_str || split_char
                 ELSE
                  v_str
               END || TRIM(v_col_typ(item));
    END LOOP;
    RETURN v_str;
  END;
  --����merge����inert����
  PROCEDURE get_merge_insert_part(p_src_owner VARCHAR2,
                                 p_src_tab   VARCHAR2,
                                 p_tar_owner VARCHAR2,
                                 p_tar_tab   VARCHAR2,
                                 v_sql       OUT clob) IS
    TYPE type_col IS RECORD(
      t_col VARCHAR2(100),
      s_col VARCHAR2(100));
    TYPE type_col_tab IS TABLE OF type_col;
    t_col_table   type_col_tab;
    v_src_link    VARCHAR2(100);
    v_tar_link    VARCHAR2(100);
    v_src_tab     VARCHAR2(100);
    v_tar_tab     VARCHAR2(100);
    v_part_insert clob;
    v_part_select clob;
    v_exe_sql     VARCHAR2(4000);
  BEGIN
    v_src_tab := p_src_tab;
    v_tar_tab := p_tar_tab;
    --�жϱ����Ƿ��dblink
    IF instr(p_src_tab, '@') > 0
    THEN
      v_src_tab  := substr(p_src_tab, 1, instr(p_src_tab, '@') - 1);
      v_src_link := substr(p_src_tab, instr(p_src_tab, '@'));
    END IF;
    IF instr(p_tar_tab, '@') > 0
    THEN
      v_tar_tab  := substr(p_tar_tab, 1, instr(p_tar_tab, '@') - 1);
      v_tar_link := substr(p_tar_tab, instr(p_tar_tab, '@'));
    END IF;
    --�������ֵ�ƥ����(������������������Ҫ����ƥ��)
    v_exe_sql := 'SELECT
       t.column_name t_col,
       s.column_name s_col
        FROM (SELECT owner,
                     table_name,
                     column_name,
                     data_type,
                     data_length,
                     column_id
                FROM all_tab_columns' || v_tar_link || '
               WHERE table_name = ''' || upper(v_tar_tab) ||
                 ''' AND owner = ''' || upper(p_tar_owner) ||
                 ''') t,
             (SELECT owner, table_name, column_name, data_type, data_length
                FROM all_tab_columns' || v_src_link || '
               WHERE table_name =  ''' || upper(v_src_tab) ||
                 ''' AND owner = ''' || upper(p_src_owner) ||
                 ''') s
       WHERE t.column_name = s.column_name AND t.data_type = s.data_type
       --AND t.data_length = s.data_length
       ORDER BY t.column_id';
    --dbms_output.put_line(v_exe_sql);
    EXECUTE IMMEDIATE v_exe_sql BULK COLLECT
      INTO t_col_table;

    --������������insert���
    FOR item IN 1 .. t_col_table.count LOOP
      v_part_insert := v_part_insert || 't.'||t_col_table(item).t_col || ',';
      v_part_select := v_part_select || 's.'||t_col_table(item).s_col || ',';
    END LOOP;
    v_sql := 'insert(' || rtrim(v_part_insert, ',') || ') values (' ||rtrim(v_part_select, ',') || ')';
    --dbms_output.put_line(v_sql);
  END;

  --����merge����update����
  PROCEDURE get_merge_update_part(p_key_col   VARCHAR2,
                                  p_src_owner VARCHAR2,
                                  p_src_tab   VARCHAR2,
                                  p_tar_owner VARCHAR2,
                                  p_tar_tab   VARCHAR2,
                                  v_sql       OUT clob) IS
    TYPE type_col IS RECORD(
      t_col VARCHAR2(100),
      s_col VARCHAR2(100));
    TYPE type_col_tab IS TABLE OF type_col;
    t_col_table   type_col_tab;
    v_src_link    VARCHAR2(100);
    v_tar_link    VARCHAR2(100);
    v_src_tab     VARCHAR2(100);
    v_tar_tab     VARCHAR2(100);
    v_part_update clob;
    v_exe_sql     VARCHAR2(4000);
  BEGIN
    v_src_tab := p_src_tab;
    v_tar_tab := p_tar_tab;
    --�жϱ����Ƿ��dblink
    IF instr(p_src_tab, '@') > 0
    THEN
      v_src_tab  := substr(p_src_tab, 1, instr(p_src_tab, '@') - 1);
      v_src_link := substr(p_src_tab, instr(p_src_tab, '@'));
    END IF;
    IF instr(p_tar_tab, '@') > 0
    THEN
      v_tar_tab  := substr(p_tar_tab, 1, instr(p_tar_tab, '@') - 1);
      v_tar_link := substr(p_tar_tab, instr(p_tar_tab, '@'));
    END IF;
    --�������ֵ�ƥ����(������������������Ҫ����ƥ��)
    v_exe_sql := 'SELECT
       t.column_name t_col,
       s.column_name s_col
        FROM (SELECT owner,
                     table_name,
                     column_name,
                     data_type,
                     data_length,
                     column_id
                FROM all_tab_columns' || v_tar_link || '
               WHERE table_name = ''' || upper(v_tar_tab) ||
                 ''' AND owner = ''' || upper(p_tar_owner) ||
                 ''') t,
             (SELECT owner, table_name, column_name, data_type, data_length
                FROM all_tab_columns' || v_src_link || '
               WHERE table_name =  ''' || upper(v_src_tab) ||
                 ''' AND owner = ''' || upper(p_src_owner) ||
                 ''') s
       WHERE t.column_name = s.column_name AND t.data_type = s.data_type
       --AND t.data_length = s.data_length
       and t.column_name not in('''||REPLACE(upper(REPLACE(p_key_col,' ','')), ',', ''',''')||''')
       ORDER BY t.column_id';
    --dbms_output.put_line(v_exe_sql);
    EXECUTE IMMEDIATE v_exe_sql BULK COLLECT
      INTO t_col_table;
    --������������insert���
    FOR item IN 1 .. t_col_table.count LOOP
      v_part_update := v_part_update || 't.' || t_col_table(item).t_col ||'=s.' || t_col_table(item).s_col || ',';
    END LOOP;
    v_sql := 'update set ' || rtrim(v_part_update, ',');
    --dbms_output.put_line(v_sql);
  END;

  --����merge����on����
  PROCEDURE get_merge_on_part(p_key_col VARCHAR2, v_sql OUT VARCHAR2) IS
    v_inst_clos_typ typ_varchar2;
  BEGIN
    v_inst_clos_typ := split(p_key_col, ',');
    FOR item IN v_inst_clos_typ.first .. v_inst_clos_typ.last LOOP
      v_sql := CASE
                 WHEN v_sql IS NOT NULL THEN
                  v_sql || ' AND '
                 ELSE
                  v_sql
               END || 't.' || TRIM(v_inst_clos_typ(item)) || '=s.' ||
               TRIM(v_inst_clos_typ(item));
    END LOOP;
    --dbms_output.put_line(v_sql);
  END;

  --����merge���
  PROCEDURE get_merge_sql(p_key_col   VARCHAR2,
                          p_src_owner VARCHAR2,
                          p_src_tab   VARCHAR2,
                          p_tar_owner VARCHAR2,
                          p_tar_tab   VARCHAR2,
                           p_sel_where VARCHAR2,
                          v_sql       OUT clob) IS
    v_on_part     VARCHAR2(4000);
    v_insert_part clob;
    v_update_part clob;
    v_partition   VARCHAR2(100);
    v_tar_tab     VARCHAR2(100);
  BEGIN
    v_tar_tab:=p_tar_tab;
    --���Ŀ����Ǵ�������,��'DW_TAB partition(part1)'���ȡ'partition(part1)'
    v_partition:=regexp_substr(upper(p_tar_tab),'PARTITION(.)+');

    if instr(p_tar_tab,' ')>0 then
       v_tar_tab:=substr(p_tar_tab,1,instr(p_tar_tab,' ')-1);
     end if;
    get_merge_on_part(p_key_col, v_on_part);
    get_merge_update_part(p_key_col, p_src_owner, p_src_tab, p_tar_owner, v_tar_tab, v_update_part);
    get_merge_insert_part(p_src_owner, p_src_tab, p_tar_owner, v_tar_tab, v_insert_part);
  if (trim(p_sel_where) is  null) then
   v_sql := 'merge/*+parallel(t,4)*/ into '|| p_tar_owner || '.' || v_tar_tab ||' '|| v_partition||' t
              using ' || p_src_owner || '.' || p_src_tab || ' s
               on(' || v_on_part || ')
              when matched then
                '|| v_update_part||'
              when not matched then
                '||v_insert_part;
    else
     v_sql := 'merge/*+parallel(t,4)*/ into '|| p_tar_owner || '.' || v_tar_tab ||' '|| v_partition||' t
              using (select * from ' || p_src_owner || '.' || p_src_tab || ' '|| p_sel_where ||') s
               on(' || v_on_part || ')
              when matched then
                '|| v_update_part||'
              when not matched then
                '||v_insert_part;
    end if;
  END;
 FUNCTION get_tab_partition_rownum(p_tab_name varchar2) return sys_refcursor is
   v_sql varchar2(30000);
   v_cursor sys_refcursor;
 begin
   for item in(select ' union all' union_part,' select '''||partition_name||''' paritioin_name,count(1) row_num from '||table_name||' partition('||partition_name||')' sql_text,rownum rn from user_tab_partitions where table_name=upper(p_tab_name) order by partition_position) loop
     v_sql:=v_sql||case when item.rn=1 then item.sql_text else item.union_part||item.sql_text end;
   end loop;
   open v_cursor for v_sql;
   return v_cursor;
 end get_tab_partition_rownum;


   --������������
  PROCEDURE get_matched_minus_sql(p_tmp_tab VARCHAR2,--�������������ʱ��
                                 p_src_owner   VARCHAR2,
                                 p_src_tab     VARCHAR2,
                                 p_src_sel_pal   integer,--��ѡ����,��ѯԴ������
                                 p_src_sel_cols  VARCHAR2,----��ѡ��������ѯԴ���ֶ�
                                 p_sel_where VARCHAR2,
                                 p_tar_sel_pal   integer,--��ѡ����,��ѯĿ�������
                                 p_tar_owner VARCHAR2,
                                 p_tar_tab   VARCHAR2,
                                 p_tar_sel_cols  VARCHAR2,--��ѡ��������ѯĿ����ֶ�
                                  p_reject_cols  VARCHAR2,--��ѡ�������ߵ����ֶ�(һ���ETL_DATE�ߵ�)
                                 p_v_sql OUT varchar2)
    IS
    TYPE type_col IS RECORD(
      t_col VARCHAR2(100),
      s_col VARCHAR2(100));
    TYPE type_col_tab IS TABLE OF type_col;
    t_col_table   type_col_tab;
    v_src_link    VARCHAR2(100);
    v_tar_link    VARCHAR2(100);
    v_src_tab     VARCHAR2(100);
    v_tar_tab     VARCHAR2(100);
    v_part_select_tar VARCHAR2(4000);
    v_part_select_src VARCHAR2(4000);
    v_exe_sql     VARCHAR2(4000);
    v_sql   VARCHAR2(4000);
    v_src_sel_pal     integer;
    v_tar_sel_pal     integer;
  BEGIN
    v_src_tab := p_src_tab;
    v_tar_tab := p_tar_tab;
    v_src_sel_pal :=nvl(p_src_sel_pal,2);
    v_tar_sel_pal :=nvl(p_tar_sel_pal,2);
    IF instr(p_src_tab, '@') > 0
    THEN
      v_src_tab  := substr(p_src_tab, 1, instr(p_src_tab, '@') - 1);
      v_src_link := substr(p_src_tab, instr(p_src_tab, '@'));
    END IF;
    IF instr(p_tar_tab, '@') > 0
    THEN
      v_tar_tab  := substr(p_tar_tab, 1, instr(p_tar_tab, '@') - 1);
      v_tar_link := substr(p_tar_tab, instr(p_tar_tab, '@'));
    END IF;
    --���δ�����ѯ�ֶκͲ����ֶΣ�����Ϊ��ͬ�ṹ���Զ�����SQL���
    if (trim(p_src_sel_cols) is null or trim(p_tar_sel_cols) is null) then
    v_exe_sql := 'SELECT
       t.column_name t_col,
       s.column_name s_col
        FROM (SELECT owner,
                     table_name,
                     column_name,
                     data_type,
                     data_length,
                     column_id
                FROM all_tab_columns' || v_tar_link || '
               WHERE table_name = ''' || upper(v_tar_tab) ||
                 ''' AND owner = ''' || upper(p_tar_owner) ||
                 ''') t,
             (SELECT owner, table_name, column_name, data_type, data_length
                FROM all_tab_columns' || v_src_link || '
               WHERE table_name =  ''' || upper(v_src_tab) ||
                 ''' AND owner = ''' || upper(p_src_owner) ||
                 ''') s
       WHERE t.column_name = s.column_name AND t.data_type = s.data_type
       ORDER BY t.column_id';

    EXECUTE IMMEDIATE v_exe_sql BULK COLLECT INTO t_col_table;

    FOR item IN 1 .. t_col_table.count LOOP
    if(instr(UPPER(p_reject_cols)|| ',', t_col_table(item).t_col|| ',') <=0 ) then
      v_part_select_tar := v_part_select_tar || t_col_table(item).t_col || ',';
      v_part_select_src := v_part_select_src || t_col_table(item).s_col || ',';
     end if;
    END LOOP;
    v_sql := 'create table ' || p_tar_owner||'.'||p_tmp_tab || ' nologging as ' ||
              ' select/*+parallel(a,'||v_src_sel_pal||')*/ ' ||
             rtrim(v_part_select_src, ',') || ' from ' ||p_src_owner||'.'||p_src_tab||' a '||p_sel_where
             ||' minus  select/*+parallel(a,'||v_tar_sel_pal||')*/ '||
             rtrim(v_part_select_tar, ',') || ' from ' ||p_tar_owner||'.'||p_tar_tab;
  -- dbms_output.put_line('v_sql1:'||v_sql);
   elsif (trim(p_src_sel_cols) is not null and trim(p_tar_sel_cols) is not null) then
      v_sql := 'create table ' || p_tar_owner||'.'||p_tmp_tab || ' nologging as ' ||
              ' select/*+parallel(a,'||v_src_sel_pal||')*/ ' ||
             rtrim(p_src_sel_cols, ',') || ' from ' ||p_src_owner||'.'||p_src_tab||' a '||p_sel_where
             ||' minus  select/*+parallel(a,'||v_tar_sel_pal||')*/ '||
             rtrim(p_tar_sel_cols, ',') || ' from ' ||p_tar_owner||'.'||p_tar_tab;
 -- dbms_output.put_line('v_sql2:'||v_sql);
   end if;
  p_v_sql:=v_sql;
  END;


  --ͬ���������ݲ�������Ŀ���
   PROCEDURE handle_tab_end(p_tab   VARCHAR2,--����
                          p_data_dt varchar2)
                            IS
  v_stepname varchar2(200);
  v_is_part  varchar2(1);--1 �Ƿ�����
  v_stat_num integer;
  v_table_owner varchar2(30);--�û���
  v_table_name varchar2(30);--����
 v_table_space VARCHAR2(30);--��ռ���
 v_part_type  VARCHAR2(1);--��������D=��M=��
  v_is_analyze VARCHAR2(1);--�Ƿ����ռ�ͳ����Ϣ
 v_last_analy_date DATE;--�ϴα��������
 v_is_move  VARCHAR2(1);--1 move
 v_index_space VARCHAR2(30);--�����ռ�
 v_is_bak_his  VARCHAR2(1);--�Ƿ�Ǩ����ʷ���� 1 Ǩ��
 v_bak_his_table_name VARCHAR2(42);--���ݱ�
 v_bak_his_freq VARCHAR2(1);--Ǩ����ʷ����Ƶ��
 v_bak_his_keep VARCHAR2(10);--��������ʱ��
 v_bak_part_dt_col VARCHAR2(30);--����(����)����
 v_bak_part_dt_exp VARCHAR2(50);--���ڱ��ʽ
 v_mov_freq VARCHAR2(1);--MOVEƵ��D=����M=��ĩQ=��ĩY=��ĩ
 v_is_recreate_tab VARCHAR2(1);--�Ƿ���Ҫ�ؽ���(MOVE�������������û�кõ�Ч�������ö����ؽ�)
 v_recreate_tab_freq VARCHAR2(1);--�ؽ�Ƶ��
 v_data_dt  varchar2(8);
 v_data_dt_d date;
 v_bak_his_end_day date; --������������
  v_opt_log_id INT;
  v_part_name varchar2(10);--��������
  v_is_baktab_index varchar2(1);--���ݱ��Ƿ񴴽�����
  v_sql varchar2(4000);
  v_sql_clob clob;
    v_bak_owhere_exp varchar2(1000);--������������
  v_where_exp  varchar2(2000);--��������
  v_is_analy_tab varchar2(1);--�Ƿ��ռ�ͳ����Ϣ��0-���ռ���1-�ռ�,ADD_BY:WUKR@20181023@������2018R1431-�Ż����ݼ��������ռ�ͳ����Ϣ������
  BEGIN
 v_table_name:= upper(p_tab);
 v_data_dt:=p_data_dt;
 v_data_dt_d:=to_date(p_data_dt,'yyyymmdd');
 v_stepname:='�����Ƿ����';
 select username into v_table_owner from user_users ;
  select is_usr_tab_exists(v_table_name) into v_stat_num from dual;
  if (v_stat_num <=0) then
  return;
  end if;

  v_stepname:='����Ƿ������ò���';
  select count(1) into v_stat_num from ct_tab_param where table_name=v_table_name and status='1';
  if (v_stat_num <=0) then
  return;
  end if;

   v_stepname:='��ֵ����';

  select
        table_space,
         part_type,
         case when is_move='0' then '0'
              when is_move='1' and UPPER(mov_freq)='D' then '1'
              when is_move='1' and UPPER(mov_freq)='M' and  to_char(add_months(trunc(to_date(v_data_dt,'yyyymmdd'),'month'),1)-1,'yyyymmdd') =v_data_dt then '1'
              when is_move='1' and UPPER(mov_freq)='Q' and  to_char(add_months(trunc(to_date(v_data_dt,'yyyymmdd'),'q'),3)-1,'yyyymmdd') =v_data_dt then '1'
              when is_move='1' and UPPER(mov_freq)='Y' and  to_char(add_months(trunc(to_date(v_data_dt,'yyyymmdd'),'yyyy'),12)-1,'yyyymmdd') =v_data_dt then '1'
              else '0' end is_move,
         index_space,
          case when is_bak_his='0' then '0'
              when is_bak_his is not null and is_bak_his<>'0' and UPPER(bak_his_freq)='D' then is_bak_his
              when is_bak_his is not null and is_bak_his<>'0' and UPPER(bak_his_freq)='M' and  to_char(add_months(trunc(to_date(v_data_dt,'yyyymmdd'),'month'),1)-1,'yyyymmdd') =v_data_dt then is_bak_his
              when is_bak_his is not null and is_bak_his<>'0' and UPPER(bak_his_freq)='Q' and  to_char(add_months(trunc(to_date(v_data_dt,'yyyymmdd'),'q'),3)-1,'yyyymmdd') =v_data_dt then is_bak_his
              when is_bak_his is not null and is_bak_his<>'0' and UPPER(bak_his_freq)='Y' and  to_char(add_months(trunc(to_date(v_data_dt,'yyyymmdd'),'yyyy'),12)-1,'yyyymmdd') =v_data_dt then is_bak_his
              else '0' end is_bak_his,
          bak_his_table_name,
         case when bak_his_keep is not null and instr(upper(bak_his_keep),'D')>1 then v_data_dt_d-to_number(substr(upper(bak_his_keep),1,instr(upper(bak_his_keep),'D')-1))
              when bak_his_keep is not null and instr(upper(bak_his_keep),'M')>1 then add_months(v_data_dt_d,-to_number(substr(upper(bak_his_keep),1,instr(upper(bak_his_keep),'M')-1)))
              when bak_his_keep is not null and instr(upper(bak_his_keep),'Q')>1 then add_months(v_data_dt_d,-3*to_number(substr(upper(bak_his_keep),1,instr(upper(bak_his_keep),'Q')-1)))
              when bak_his_keep is not null and instr(upper(bak_his_keep),'Y')>1 then add_months(v_data_dt_d,-12*to_number(substr(upper(bak_his_keep),1,instr(upper(bak_his_keep),'Y')-1)))
              ELSE null end  as bak_his_end_day,
          bak_part_dt_col,
          upper(bak_part_dt_exp) bak_part_dt_exp,
         IS_BAKTAB_INDEX,
              case when is_recreate_tab='0' then '0'
              when is_recreate_tab='1' and UPPER(recreate_tab_freq)='D' then '1'
              when is_recreate_tab='1' and UPPER(recreate_tab_freq)='M' and  to_char(add_months(trunc(to_date(v_data_dt,'yyyymmdd'),'month'),1)-1,'yyyymmdd') =v_data_dt then '1'
              when is_recreate_tab='1' and UPPER(recreate_tab_freq)='Q' and  to_char(add_months(trunc(to_date(v_data_dt,'yyyymmdd'),'q'),3)-1,'yyyymmdd') =v_data_dt then '1'
              when is_recreate_tab='1' and UPPER(recreate_tab_freq)='Y' and  to_char(add_months(trunc(to_date(v_data_dt,'yyyymmdd'),'yyyy'),12)-1,'yyyymmdd') =v_data_dt then '1'
              else '0' end is_recreate_tab,
           a.bak_owhere_exp,
           nvl(a.is_analy_tab,'0') --�Ƿ��ռ�ͳ����Ϣ��0-���ռ���1-�ռ�,Ĭ�ϲ��ռ���ADD_BY:WUKR@20181023@������2018R1431-�Ż����ݼ��������ռ�ͳ����Ϣ������
    into  v_table_space,
         v_part_type,
         v_is_move,
         v_index_space,
         v_is_bak_his,
         v_bak_his_table_name,
         v_bak_his_end_day,
         v_bak_part_dt_col,
         v_bak_part_dt_exp,
         v_is_baktab_index,
         v_is_recreate_tab,
         v_bak_owhere_exp,
         v_is_analy_tab
    from ct_tab_param a
         inner join   user_tables b on upper( a.table_name)=b.table_name
   where   a.table_name = v_table_name
     and a.status = '1';


  v_stepname:='�������ͨ���Ƿ�����';

  select count(1) into v_stat_num  from user_tab_partitions where table_name=v_table_name;
   if(v_stat_num>=1) then
  v_is_part:='1';
  end if;


   v_stepname:='������ʷ����ǰ��鱸�ݱ�';

   if(v_is_bak_his <>'0' and v_bak_his_table_name is not null) then
            --����������ڲ�����ĩ�����ұ����·����ҷ��������ֶ��뱸���ֶ�һ�£��������������Ϊ����ĩ
            if( v_bak_his_end_day is not null and v_bak_his_end_day <> last_day(v_bak_his_end_day) and v_part_type='M'   )
            THEN
            v_bak_his_end_day :=trunc(to_date(v_data_dt,'yyyymmdd'),'month')-1;
            end if;
     if(v_is_bak_his ='2') then
     v_bak_his_end_day:=v_data_dt_d;
     end if;
   v_bak_his_table_name:=replace(replace(replace(upper(v_bak_his_table_name) ,
                  '$[YYYYMMDD]$', to_char(v_bak_his_end_day ,'yyyymmdd'))
                  ,'$[YYYYMM]$',to_char(v_bak_his_end_day, 'yyyymm'))
                  ,'$[YYYY]$',to_char(v_bak_his_end_day, 'yyyy'));
   --�жϱ��ݱ��Ƿ���ڣ�����������򴴽��±�
/*   dbms_output.put_line('v_bak_his_table_name:'||v_bak_his_table_name);
     dbms_output.put_line('v_bak_his_end_day:'||v_bak_his_end_day);*/
   select is_usr_tab_exists(v_bak_his_table_name) into v_stat_num from dual;
    if (v_stat_num <=0) then
     vers_tools.copy_table(v_table_name,v_bak_his_table_name);
       if(v_is_baktab_index='1') then
        vers_tools.copy_indexes(v_table_name,v_bak_his_table_name,null,v_index_space);
         end if;
     end if;
   end if;

     --�����������ڲ��ֱ��ݺ󣬽������ݵ�����ɾ��
  if(v_is_bak_his='1' and v_bak_his_end_day is not null) then
  v_stepname:='���ݷ�����ֵ';
    IF v_part_type = 'M' THEN
           v_part_name := 'P_' ||to_char(v_bak_his_end_day,'yyyymm') ;
       ELSE
           v_part_name := 'P_' ||to_char(v_bak_his_end_day,'yyyymmdd') ;
      END IF;
      --20180110 �׻��� ���and v_part_type is not null ���Ʒ����ڷ����������ͨ��ʽ����
     if(v_is_part='1' and v_part_type is not null) then
       v_stepname:='������Ǩ����ʷ����';
          for t_tab in (select table_name, partition_name ,high_value,tablespace_name from user_tab_partitions  where table_name=v_table_name and partition_name<=v_part_name)
               loop
               begin
               if( v_bak_his_table_name is not null) then
               --��鱸�ݱ��Ƿ��з���
               select count(1) into v_stat_num  from user_tab_partitions where table_name=v_bak_his_table_name and partition_name=t_tab.partition_name;
                if(v_stat_num<=0  ) then
                --û�з����򴴽�����
                v_sql:='alter table '||v_bak_his_table_name|| ' ADD PARTITION ' ||t_tab.partition_name|| ' values  less than ('||substr(t_tab.high_value,1,4000)||
                     ') TABLESPACE ' || nvl(v_table_space,t_tab.tablespace_name);
               execute immediate v_sql;
                end if;
                --ɾ�����ݱ����ݣ�֧������
                v_sql:='alter table '||v_bak_his_table_name|| ' truncate PARTITION ' ||t_tab.partition_name|| ' Update Global Indexes';
               execute immediate v_sql;
                --���뱸������
                  VERS_TOOLS.get_matched_inser_sql(v_table_owner, v_table_name,4,null,' partition ('||t_tab.partition_name||')', v_table_owner, v_bak_his_table_name,4,null, v_sql_clob);
                 execute immediate v_sql_clob;
                end if;
                 --ɾ����������
                  select count(1) into v_stat_num  from user_tab_partitions where table_name=v_table_name;
                  if(v_stat_num>=2 ) then
                  v_sql:='alter table '||v_table_name|| ' DROP PARTITION ' ||t_tab.partition_name|| ' Update Global Indexes';
                  --v_sql:='alter table '||v_table_name|| ' DROP PARTITION ' ||t_tab.partition_name;
                  else
                   v_sql:='alter table '||v_table_name|| ' truncate PARTITION ' ||t_tab.partition_name|| ' Update Global Indexes';
               end if;
               execute immediate v_sql;
                commit;
                 --���±�������
                update ct_tab_param set LAST_BAK_DATE=sysdate where table_name=v_table_name;
                commit;
                 end;
               end loop;
     else
     v_stepname:='��ͨ��Ǩ����ʷ����';
     --ɾ�����ݱ����ݣ�֧������
     if(v_bak_his_table_name is not null and v_bak_part_dt_col is not null and v_bak_part_dt_exp is not null) then
      v_bak_part_dt_exp:=  replace(replace(replace(v_bak_part_dt_exp ,
                  '$[YYYYMMDD]$', to_char(v_bak_his_end_day ,'yyyymmdd'))
                  ,'$[YYYY-MM-DD]$',to_char(v_bak_his_end_day, 'yyyy-mm-dd'))
                  ,'$[YYYY/MM/DD]$',to_char(v_bak_his_end_day, 'yyyy/mm/dd'));
    /* DBMS_OUTPUT.put_line(v_bak_part_dt_col);
     DBMS_OUTPUT.put_line(v_bak_part_dt_exp);*/
    v_sql:='delete from '||v_bak_his_table_name||' where '||v_bak_part_dt_col||' >=(select min('||v_bak_part_dt_col||')  from  '||v_table_name||') ';
    execute immediate v_sql;
    commit;
     --���뱸������
      --20180110 �׻��� ����������������
     v_where_exp :=' where '||v_bak_part_dt_col||' <='||v_bak_part_dt_exp;
     if(trim(v_bak_owhere_exp) is not null ) then
     v_where_exp :=v_where_exp||' and '||v_bak_owhere_exp;
     end if;
     VERS_TOOLS.get_matched_inser_sql(v_table_owner, v_table_name,4,null,' where '||v_bak_part_dt_col||' <='||v_bak_part_dt_exp, v_table_owner, v_bak_his_table_name,4,null, v_sql_clob);
     execute immediate v_sql_clob;
     commit;
     end if;
     --ɾ����������
     v_sql:='delete from '||v_table_name||' where '||v_bak_part_dt_col||' <='||v_bak_part_dt_exp;
    execute immediate v_sql;
    commit;
    --���±�������
    update ct_tab_param set LAST_BAK_DATE=sysdate where table_name=v_table_name;
    commit;
     end if;
 end if;
   if(v_is_bak_his='2') then
          if( v_bak_his_table_name is not null) then
            v_sql:='truncate table '||v_bak_his_table_name;
            execute immediate v_sql;
            --���뱸������
             --20180110 �׻��� ����������������
            v_where_exp:='';
             if(trim(v_bak_owhere_exp) is not null ) then
              v_where_exp :=' where '||v_bak_owhere_exp;
              end if;
             VERS_TOOLS.get_matched_inser_sql(v_table_owner, v_table_name,4,null,null, v_table_owner, v_bak_his_table_name,4,null, v_sql_clob);
            execute immediate v_sql_clob;
            commit;
             update ct_tab_param set LAST_BAK_DATE=sysdate where table_name=v_table_name;
             commit;
          end if;
  end if;

  --move��
  v_stepname:='move��';
  --����MOVE�����
  if(v_is_move='1') then
  move_table(v_table_name,v_table_space,4);
  update ct_tab_param set LAST_MOVE_DATE=sysdate where table_name=v_table_name;
  commit;
  end if;



  v_stepname:='�ؽ���';
  if (v_is_recreate_tab ='1' ) then
  --�����ؽ������
   vers_tools.recre_table(v_table_name);
  end if;

   v_stepname:='�������,�ؽ�����';
  --�����ؽ���������
   vers_tools.rebuild_index(v_table_name,v_index_space,4);
  --�ռ�ͳ����Ϣ
  --�ж��Ƿ���Ҫ�ռ�ͳ����Ϣ��0-���ռ���1-�ռ�,ADD_BY:WUKR@20181023@������2018R1431-�Ż����ݼ��������ռ�ͳ����Ϣ������
  if (v_is_analy_tab ='1' ) then
    analyze_table(v_table_name,'0',v_stat_num);
    if(v_stat_num=1) then
     update ct_tab_param set LAST_ANALY_DATE=sysdate where table_name=v_table_name;
     commit;
    end if;
  end if;
   --�������½�����
     if(v_is_part='1' and v_part_type is not null) then
     --�������·��������շ�����ֵ���������Լ��½��������ʽ
     IF v_part_type = 'M' THEN
           v_part_name := 'P_' ||to_char(add_months(v_data_dt_d,1),'yyyymm') ;
           if(v_bak_part_dt_exp is not null) then
           v_bak_part_dt_exp:=  replace(replace(replace(v_bak_part_dt_exp ,
                  '$[YYYYMMDD]$', to_char(add_months(v_data_dt_d,1)+1 ,'yyyymmdd'))
                  ,'$[YYYY-MM-DD]$',to_char(add_months(v_data_dt_d,1)+1, 'yyyy-mm-dd'))
                  ,'$[YYYY/MM/DD]$',to_char(add_months(v_data_dt_d,1)+1, 'yyyy/mm/dd'));
           end if;
       ELSE
           v_part_name := 'P_' ||to_char(v_data_dt_d+1,'yyyymmdd') ;
            if(v_bak_part_dt_exp is not null) then
           v_bak_part_dt_exp:=  replace(replace(replace(v_bak_part_dt_exp ,
                  '$[YYYYMMDD]$', to_char(v_data_dt_d+2 ,'yyyymmdd'))
                  ,'$[YYYY-MM-DD]$',to_char(v_data_dt_d+2, 'yyyy-mm-dd'))
                  ,'$[YYYY/MM/DD]$',to_char(v_data_dt_d+2, 'yyyy/mm/dd'));
           end if;
      END IF;
      dbms_output.put_line('v_part_name:'||v_part_name);
       dbms_output.put_line('v_bak_part_dt_exp:'||v_bak_part_dt_exp);
      --�ж��Ƿ��Ѿ��з��������û�����½�
      if(v_bak_part_dt_exp is not null ) then
      select count(1) into v_stat_num from user_tab_partitions where   table_name=v_table_name and partition_name=v_part_name;
       dbms_output.put_line('v_stat_num:'||v_stat_num);
      if(v_stat_num<=0) then
       v_sql :='ALTER TABLE ' || v_table_name || ' ADD PARTITION ' ||
                     v_part_name || ' values less than ( '||v_bak_part_dt_exp|| ') tablespace '||v_table_space;
         dbms_output.put_line('v_sql:'||v_sql);
       execute immediate v_sql;
      end if;
      end if;
     end if;
   commit;
  end handle_tab_end;

 --MOVE��
   PROCEDURE move_table(p_tab   VARCHAR2,--����
                          p_table_space varchar2, --��ռ�
                         p_parallel integer --���ж�
                         ) IS
    v_tab    VARCHAR2(30);
    v_table_space VARCHAR2(30);
    v_stat_num number;
    v_is_part varchar2(1);
    v_sql varchar2(3000);
    v_parallel integer;
    v_lob_cols VARCHAR2(200);
    v_col_type typ_varchar2;
  BEGIN
  v_tab:=upper(p_tab);
   v_table_space:=upper(p_table_space);
    v_parallel:=nvl(p_parallel,4);
  --�����Ƿ����
     select is_usr_tab_exists(v_tab) into v_stat_num from dual;
  if (v_stat_num <=0) then
  return;
  end if;
  --����Ƿ������
  select count(1) into v_stat_num  from user_tab_partitions where table_name=v_tab;
   if(v_stat_num>=1) then
     v_is_part:='1';
  end if;
 select count(1) into v_stat_num  from USER_TAB_COLUMNS where table_name=v_tab  and data_type like '%LOB%';
   if(v_stat_num>=1) then
     v_sql :='SELECT COLUMN_NAME FROM USER_TAB_COLUMNS where table_name='''||v_tab||'''  and data_type like ''%LOB%''';
     execute immediate v_sql BULK COLLECT  INTO v_col_type;
     v_lob_cols:= vers_tools.col_t_row(v_col_type,',') ;
   --  dbms_output.put_line(v_lob_cols);
    end if;
 if(v_is_part='1') then
   for t_tab in (select table_name, partition_name ,tablespace_name from user_tab_partitions  where table_name=v_tab )
               loop
               --move
               v_sql :='alter table '||t_tab.table_name ||' move partition '||t_tab.partition_name||' tablespace '||nvl(v_table_space,t_tab.tablespace_name)||'  Update Global Indexes parallel '||v_parallel||'  nologging' ;
               if(v_lob_cols is not null) then
               v_sql :=v_sql||' lob('||v_lob_cols||') store as ( tablespace '||nvl(v_table_space,t_tab.tablespace_name)||')';
               end if;
                execute immediate v_sql;
                --�ؽ��ֲ�����
               -- alter table dm_ims_transaction modify partition p_20170820 rebuild unusable local indexes;
                v_sql :='alter table '||t_tab.table_name ||' modify partition '||t_tab.partition_name||' rebuild unusable local indexes ' ;
                --dbms_output.put_line(v_sql);
                execute immediate v_sql;
               end loop;

 else
 --move ��ͨ��
 if(v_table_space is null ) then
 select tablespace_name into v_table_space from user_tables where   table_name=v_tab ;
 end if;
  v_sql :='alter table '||v_tab ||' move tablespace '||v_table_space;
   if(v_lob_cols is not null) then
   v_sql :=v_sql||' lob('||v_lob_cols||') store as ( tablespace '||v_table_space||')';
  end if;
  execute immediate v_sql;
 end if;
   --�ؽ�����
vers_tools.rebuild_index(v_tab,v_table_space,4);
  end move_table;


  --�ؽ�����
   PROCEDURE rebuild_index(p_tab   VARCHAR2,--����
                         p_index_space varchar2, --�����ռ�
                          p_parallel integer --���ж�
                          )
      IS
    v_tab    VARCHAR2(30);
    v_index_space VARCHAR2(30);
    v_stat_num number;
    v_is_part varchar2(1);
    v_sql varchar2(3000);
    v_parallel integer;
  BEGIN
  v_tab:=upper(p_tab);
   v_index_space:=upper(p_index_space);
    v_parallel:=nvl(p_parallel,4);
  --�����Ƿ����
     select is_usr_tab_exists(v_tab) into v_stat_num from dual;
      if (v_stat_num <=0) then
      return;
      end if;
  --�ؽ�ȫ������
    for t_ind in (select index_name,tablespace_name from user_indexes  where table_name=v_tab and status ='UNUSABLE' )
               loop
               --alter index DM_IMS_TRANSACTION_IDX3 rebuild tablespace DM_TS_ind parallel 4 online nologging;
                v_sql :='alter index '||t_ind.index_name ||' rebuild tablespace '||nvl(v_index_space,t_ind.tablespace_name)||' parallel '||v_parallel||' online nologging' ;
                execute immediate v_sql;
               end loop;
 --�ؽ��ֲ�����
   for t_ind in (select b.index_name,b.partition_name,b.tablespace_name from user_indexes a ,USER_IND_PARTITIONS b  where a.table_name=v_tab and b.status ='UNUSABLE' )
               loop
               -- alter index DM_IMS_TRANSACTION_IDX4 rebuild partition p_20170820 tablespace DM_TS_IND;
                v_sql :='alter index '||t_ind.index_name  ||' rebuild partition '||t_ind.partition_name||' tablespace '||nvl(v_index_space,t_ind.tablespace_name);
                execute immediate v_sql;
                 end loop;


  end rebuild_index;

   PROCEDURE recre_table(p_tab   VARCHAR2--����
                          ) is

    v_tab    VARCHAR2(30);
    v_stat_num number;
    v_is_part varchar2(1);
    v_sql_clob clob;
    v_sql varchar2(2000);
    v_table_owner  VARCHAR2(30);
  v_tmp_tbl_name1 varchar2(30) := VERS_TOOLS.get_temp_tablename();
  v_tmp_tbl_name2 varchar2(30) := VERS_TOOLS.get_temp_tablename();
  BEGIN
  v_tab:=upper(p_tab);
     --�����Ƿ����
     select is_usr_tab_exists(v_tab) into v_stat_num from dual;
  if (v_stat_num <=0) then
  return;
  end if;
  select username into v_table_owner from user_users ;
  --����Ƿ������
  select count(1) into v_stat_num  from user_tab_partitions where table_name=v_tab;
   if(v_stat_num>=1) then
     v_is_part:='1';
  end if;
   --���±�
  vers_tools.copy_table(v_tab,v_tmp_tbl_name1);
  --��������
    if(v_is_part='1' ) then
        --�������������
          for t_tab in (select table_name, partition_name ,high_value,tablespace_name from user_tab_partitions  where table_name=v_tab )
               loop
                --���뱸������
                 VERS_TOOLS.get_matched_inser_sql(v_table_owner, v_tab,4,null,' partition ('||t_tab.partition_name||')', v_table_owner, v_tmp_tbl_name1,4,null, v_sql_clob);
                 execute immediate v_sql_clob;
                 COMMIT;
               end loop;
     else
     VERS_TOOLS.get_matched_inser_sql(v_table_owner, v_tab,4,null,null, v_table_owner, v_tmp_tbl_name1,4,null, v_sql_clob);
      execute immediate v_sql_clob;
      COMMIT;
    end if;

  --�ؽ�����
  vers_tools.copy_indexes(v_tab,v_tmp_tbl_name1,'4',null);
  --������
  v_sql :='rename ' ||v_tab||' to '||v_tmp_tbl_name2;
  execute immediate v_sql;
   v_sql :='rename ' ||v_tmp_tbl_name1||' to '||v_tab;
  execute immediate v_sql;
  --ɾ��ԭ��
  vers_tools.drop_tab_no_interruption(v_tmp_tbl_name2);

  end recre_table;



      --�����
    PROCEDURE analyze_table( p_tab   VARCHAR2,--����
                             is_must varchar2,--�Ƿ����������� 1 �� 0 ��
                          status OUT number)  is

    v_tab    VARCHAR2(30);
    v_stat_num number;
    v_is_part varchar2(1);
    v_is_analyze varchar2(1);
    v_sql varchar2(2000);
    v_table_owner  VARCHAR2(30);
  BEGIN
  v_tab:=upper(p_tab);
  status:=0;
     --�����Ƿ����
     select is_usr_tab_exists(v_tab) into v_stat_num from dual;
  if (v_stat_num <=0) then
  return;
  end if;
  select username into v_table_owner from user_users ;
  --����Ƿ������
  select count(1) into v_stat_num  from user_tab_partitions where table_name=v_tab;
   if(v_stat_num>=1) then
     v_is_part:='1';
  end if;
   if(v_is_part='1' ) then
    for analy_part   in (select partition_name from dba_tab_statistics  where  owner=v_table_owner and table_name=v_tab  and object_type='PARTITION'
        and  ( last_analyzed is  null or stale_stats='YES' OR stale_stats is null or is_must ='1' ) )
        loop
         dbms_stats.gather_table_stats(ownname => v_table_owner,tabname => v_tab,partname => analy_part.partition_name , estimate_percent => dbms_stats.AUTO_SAMPLE_SIZE,degree => dbms_stats.AUTO_DEGREE,cascade => true);
          status:=1;
          end loop;
    else
    select case when last_analyzed is  null or stale_stats='YES' OR stale_stats is null or is_must ='1' then '1' else '0'  end is_analyze   into v_is_analyze from dba_tab_statistics  where  owner=v_table_owner and table_name=v_tab  and object_type='TABLE' ;
     if(v_is_analyze = '1' ) then
    dbms_stats.gather_table_stats(ownname => v_table_owner,tabname => v_tab,estimate_percent => dbms_stats.AUTO_SAMPLE_SIZE,degree => dbms_stats.AUTO_DEGREE,cascade => true);
     status:=1;
     end if;
    end if;

  end analyze_table;
end VERS_TOOLS;
