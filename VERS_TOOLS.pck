create or replace package body odsbdata.VERS_TOOLS is
  /******************************************************************************
   * TITLE      : 广州银行数据集市沉淀层-工具包
   * DESCRIPTION: 常用工具，后续常用的工具函数，存储过程添加到这个包里供大家使用
   * AUTHOR     : 米辉翀
   * VERSION    : 1.0
   * DATE       : 2017-06-07
   * MODIFY     : 后续修改版本在此列出
               20171205 米辉翀 增加MOVE表操作
             20180108 米辉翀 handle_tab_end 优化非日期分区表采用普通表方式备份 增加其他条件字段 优化Lob字段move操作
             20180109 米辉翀 move_table  优化新增含lob字段的Move 支持
             20181023 吴康荣 handle_tab_end 优化收集统计信息逻辑,根据CT_TAB_PARAM表IS_ANALY_TAB配置判断是否需要收集统计信息@需求编号2018R1431
  ******************************************************************************/
  --全局变量->开始
  V_PAG_NAME varchar(30) := 'DWTOOLS';
  V_MAIN_VERSION varchar(10) := 'v1.0.0';
  V_LOG_TITLE varchar(64) := V_PAG_NAME||V_MAIN_VERSION;
  --全局变量->结束

  --返回左右连接的SQL表达式->开始
  function get_l_r_conn_sql(col_list varchar2, --列名列表
                             left_tab varchar2, --左表
                             right_tab varchar2 --右表
                          ) return varchar2 is
     v_ret varchar2(2048);
     v_col_list varchar2(2048);
     v_col_list_left varchar2(2048);
     v_the_col varchar2(30);
     v_tmp1  varchar2(512);
  begin
     --清除多余空格
     v_col_list := col_list;
     v_col_list := ','||trim(v_col_list)||',';
     v_col_list := replace(v_col_list,' ','');

     --生成左右表达式
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
  --返回左右连接的SQL表达式->结束

  --返回用户表索引是否存在->开始
  function is_usr_idx_exists(tab_name varchar2, --表名
                             idx_name varchar2 --索引名
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
  --返回用户表索引是否存在->结束

  --返回用户表是否存在->开始
  function is_usr_tab_exists(tab_name varchar2 --表名
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
  --返回用户表是否存在->结束

  --删除操作日志->开始
  procedure del_opt_log_inf(keep_days int) is --保留天数
     v_error varchar(2048);
     v_once_rownum int := 1000;
     v_max_log_id number;
  begin
     select max(LOG_ID) into v_max_log_id from V_OPT_LOG_INF where LOG_TIME < (sysdate-keep_days);
     if v_max_log_id is not null then
        delete from V_OPT_LOG_INF where LOG_ID <= v_max_log_id and rownum < v_once_rownum;
        opt_file_log(V_LOG_TITLE,'删除操作日志行数',to_char(SQL%ROWCOUNT));
     end if;
  exception
    when others then
      v_error := '['|| SQLERRM || ']v_max_log_id['||to_char(v_max_log_id)||']';
      opt_file_errlog(V_LOG_TITLE,'del_opt_log_inf',v_error,1);
      raise;
  end del_opt_log_inf;
  --删除操作日志->结束

  --返回SQL日期表达式->开始
  function get_sql_date2(date_value date, --日期值
                         date_fmt varchar2    --日期格式
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
      raise_application_error (-20105, '无效日期'||date_value||'或无效格式'||date_fmt);
      return null;
      raise;
  end;
  --返回SQL日期表达式->结束

  --返回SQL日期表达式->开始
  function get_sql_date(date_value varchar2, --日期值
                        date_fmt varchar2    --日期格式
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
      raise_application_error (-20105, '无效日期'||date_value||'或无效格式'||date_fmt);
      return null;
      raise;
  end;
  --返回SQL日期表达式->结束

  --返回SQL数值表达式->开始
  function get_sql_num(num_value varchar2 --数值
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
      raise_application_error (-20104, '无效数值'||num_value);
      return null;
      raise;
  end;

  --返回SQL字符串表达式->开始
  function get_sql_str(str_value varchar2 --字符串值
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
  --返回SQL字符串表达式->结束

  --返回日期差->开始
  function date_diff(datepart varchar2, --日期部分(参照MS-SQL SERVER)
                   startdate  date, --开始日期
                   enddate  date --结束日期
                          ) return int is
    v_ret number ;
    v_error varchar2(2048);
    ERR_DATE_PART exception;
  begin
    v_ret := to_number(enddate-startdate);
    case datepart
      when 'yy' then      --年
         v_ret := v_ret/365;
      when 'qq' then      --季
         v_ret := v_ret/365/4;
      when 'mm' then      --月
         v_ret := v_ret/12;
      when 'dd' then      --日
         null;
      when 'wk' then      --周
         v_ret := v_ret/7;
      when 'hh' then      --时
         v_ret := v_ret*24;
      when 'mi' then      --分
         v_ret := v_ret*24*60;
      when 'ss' then      --秒
         v_ret := v_ret*24*60*60;
      else
        raise ERR_DATE_PART;
    end case;
    return v_ret;
  exception
    when ERR_DATE_PART then
      v_error := '无效日期部分'||datepart;
      raise_application_error (-20103, v_error);
    when others then
      return null;
      raise;
  end;
  --返回日期差->结束

  --读取字符串的中间部分->开始
  function get_str(src_str varchar2, --源字符串
                   begin_str varchar2, --开始串
                   end_str  varchar2 --结束串
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
  --读取字符串的中间部分->结束

  --读取一个代码值->开始
  procedure get_code_value(CD_ENAME varchar2, cd_value out varchar2) as
     v_error varchar(2048);
     v_sql varchar(2048);
  begin
     v_sql := 'select CD_VALUE from CT_MAIN_CODE_INFO where CD_ENAME = :1 ';
     execute immediate v_sql into cd_value using CD_ENAME;
  exception
    when NO_DATA_FOUND then
      v_error := '代码['||CD_ENAME||']不存在';
      opt_file_errlog(V_LOG_TITLE,'get_code_value',v_error,1);
      raise_application_error (-20102, v_error);
    when others then
      v_error := '['||v_sql||'][' || SQLERRM || ']';
      opt_file_errlog(V_LOG_TITLE,'get_code_value',v_error,1);
      raise;
  end get_code_value;
  --读取一个代码值->结束
  --------------------------------------------------------------------

 --设置一个代码值->开始
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
      v_error := '代码['||CD_ENAME||']不存在';
      opt_file_errlog(V_LOG_TITLE,'set_code_value',v_error,1);
      rollback;
      raise_application_error (-20102, v_error);
    when others then
      v_error := '['||v_sql||'][' || SQLERRM || ']';
      opt_file_errlog(V_LOG_TITLE,'set_code_value',v_error,1);
      rollback;
      raise;
  end set_code_value;
  --设置一个代码值->结束

  --操作日志，打印到文件->开始
  procedure opt_file_log(file_title varchar2,log_title varchar2, log_inf clob) as
    v_log_level  varchar(10) := '信息';
    v_file_title  varchar(100);
    v_log_return number := 0;

  begin
    v_file_title  := upper(file_title) || '.' || to_char(sysdate, 'yyyymmdd');
    v_log_return := write_log_file(v_log_level, log_title , v_file_title, log_inf);

  end opt_file_log;
  --操作日志，打印到文件->结束
  --------------------------------------------------------------------
  --操作错误日志，打印到文件->开始
  procedure opt_file_errlog(file_title varchar2,log_title varchar2, errlog_inf clob, is_errlog_only integer) as
    v_log_level  varchar(10) := '错误';
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
  --操作错误日志，打印到文件->结束
  --------------------------------------------------------------------
  --开始一个操作日志->开始
  procedure opt_db_begin(opt_title  varchar2, --操作标题
                         opt_desc   varchar2, --操作内容
                         etl_dt    varchar2,--数据日期
                         ret_log_id out number   --返回日志标识
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
  --打印信息到文件->结束
  --------------------------------------------------------------------
  --结束一个操作日志->开始
  procedure opt_db_end(opt_log_id number, --日志的标识
                       row_count number,--影响行数
                       opt_res   clob, --操作结果
                       status number --状态 0正常 1 异常
                       ) as
    err_opt_log_id_not_found exception;
    v_error varchar(4000);
    v_cnt int;
    v_row_count number;
    v_opt_res clob;
    v_exe_status  number;
     V_MISECOND      INTEGER;                 --耗时：毫秒
     V_SECONDS       INTEGER;                 --耗时：秒
     V_MINUTES       INTEGER;                 --耗时：分
     V_HOURS         INTEGER;                 --耗时：时
     V_DAYS          INTEGER;                 --耗时：天
     V_EXEC_TIME_STR VARCHAR2(30);            --执行时间
     V_START_TIME     TIMESTAMP;               --上次任务时间
     V_CURRENT_TM  TIMESTAMP;
      V_EXEC_TIME_DES VARCHAR2(30);            --任务耗时，描述
     V_EXEC_TIME_S   NUMBER(30, 6);           --任务耗时，时间戳，秒
  begin
    select count(*),min(START_TIME) into v_cnt,V_START_TIME from V_OPT_LOG_INF where log_id=opt_log_id;
    if v_cnt=0 then
       raise err_opt_log_id_not_found;
    end if;
    v_opt_res := opt_res;
    v_row_count:=row_count;
    v_exe_status:=status;
    V_CURRENT_TM:= CURRENT_TIMESTAMP;
    --获取执行时间
     SELECT
       TO_CHAR(V_CURRENT_TM - V_START_TIME) INTO V_EXEC_TIME_STR
     FROM DUAL;

     V_MISECOND := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, INSTR(V_EXEC_TIME_STR,' ')+10,3));  -- 毫秒
     V_SECONDS  := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, INSTR(V_EXEC_TIME_STR,' ')+7,2));   -- 秒
     V_MINUTES  := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, INSTR(V_EXEC_TIME_STR,' ')+4,2));   -- 分
     V_HOURS    := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, INSTR(V_EXEC_TIME_STR,' ')+1,2));   -- 时
     V_DAYS     := TO_NUMBER(SUBSTR(V_EXEC_TIME_STR, 1, INSTR(V_EXEC_TIME_STR,' ')));    -- 天

     V_EXEC_TIME_DES :=V_DAYS||'天'||V_HOURS||'时'||V_MINUTES||'分'||V_SECONDS||'秒'||V_MISECOND||'毫秒';

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
      v_error := '日志标识不存在';
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
  --打印信息到文件->开始
  function write_log_file(log_level varchar2, --日志级别
                          log_title  varchar2, --日志标题
                          file_title varchar2, --文件名
                          log_inf   clob --日志内容
                          ) return number is
    fp UTL_FILE.file_type;
  begin
    fp := UTL_FILE.fopen('LOG_DIR', file_title, 'a', 16384 );

    /* 输入格式为[会话][时间][级别][标题][内容] */
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
  --打印信息到文件->结束


  --返回MD5加密结果->开始
  function get_md5(src_str varchar2 --源串
                          ) return varchar2 is
  begin
     RETURN Utl_Raw.Cast_To_Raw(DBMS_OBFUSCATION_TOOLKIT.MD5(input_string => src_str));
  exception
     when others then
        return null;
  end;
  --返回MD5加密结果->结束


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

  --返回解密字符串，用于解密
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

  --返回解密串->结束
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
  --如果源标表没有索引，直接返回
  if v_idx_count = 0 then
    return;
  end if;
  for item in (select rownum rn,table_owner,index_name,table_name,tablespace_name,partitioned,regexp_substr(to_char(substr(dbms_metadata.get_ddl('INDEX',a.index_name),1,4000)),'CREATE(.)*') idx_cmd from user_indexes a where table_name=upper(v_orig_table) AND INDEX_TYPE<>'LOB') loop
    v_idx_name:=substr('idx'||item.rn||'_'||v_tar_table,1,30);

    --在中间" ON "把DDL语句切断成两部分
    v_first_part:=substr(item.idx_cmd,1,instr(item.idx_cmd,' ON '));
    v_last_part:=substr(item.idx_cmd,instr(item.idx_cmd,' ON '),1000);

    --第一部分,用目标表的索引名称替换源表的索引名称
    --第二部分,用目标表名称替换源表名称
    v_first_part:= regexp_replace(srcstr => v_first_part,pattern => '"'||item.index_name||'"',replacestr => v_idx_name);
    v_last_part:= regexp_replace(srcstr => v_last_part,pattern => '"'||item.table_name||'"',replacestr => v_tar_table);

    --替换索引和表的owner
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

--按照某个表的表结构创建另外一个表
 PROCEDURE copy_table(p_orig_table VARCHAR2, --参考表
                         p_tar_table  VARCHAR2 --目标表
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
 --关闭表索引、外键等关联

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS', FALSE);

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'REF_CONSTRAINTS', FALSE);

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS_AS_ALTER', FALSE);
--关闭存储、表空间属性

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'STORAGE', FALSE);

-- DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'TABLESPACE', FALSE);

--关闭创建表的PCTFREE、NOCOMPRESS等属性

 DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', FALSE);


select regexp_substr(dbms_metadata.get_ddl('TABLE',v_orig_table),'CREATE(.)*') ,dbms_metadata.get_ddl('TABLE',v_orig_table)
 into v_orig_part,v_tab_ddl  from dual;

   --用目标表名称替换源表名称
    v_tar_part:= regexp_replace(srcstr => v_orig_part,pattern => '"'||v_orig_table||'"',replacestr => v_tar_table);
   -- dbms_output.put_line(v_tar_part);
    --用替换好的表替换建表语句
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
    --删掉约束
    FOR item IN (SELECT constraint_name
                   FROM all_constraints
                  WHERE owner = upper(p_owner) AND table_name = upper(p_tab)) LOOP
      EXECUTE IMMEDIATE 'alter table ' || p_owner || '.' || p_tab ||
                        ' drop constraint ' || item.constraint_name ||
                        ' cascade';
    END LOOP;
    --删掉索引
    FOR item IN (SELECT index_name
                   FROM all_indexes
                  WHERE owner = upper(p_owner) AND table_name = upper(p_tab)) LOOP
      EXECUTE IMMEDIATE 'drop index ' || p_owner || '.' || item.index_name;
    END LOOP;
  END drop_all_cons_idx;

---获取插入语句，参数源表用户，源表，查询并发数，查询字段，查询条件，目标表用户，目标表，插入并发数，插入字段，返回SQL
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
    --如果未传入查询字段和插入字段，则认为是同结构表，自动生成插入语句
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
                                   p_sel_cols  VARCHAR2,--查询字段
                                    p_sel_where VARCHAR2,
                                   p_tar_owner   VARCHAR2,
                                   p_tar_tab     VARCHAR2,
                                    p_ins_pal   integer,
                                    p_ins_cols  VARCHAR2,--插入字段
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
    --如果目标表是带分区的,如'DW_CUST_ACCT_INFO_PER partition(part_ccbs_sa)'则截取'partition(part_ccbs_sa)'
   v_partition:=regexp_substr(upper(p_tar_tab),'PARTITION(.)+');

   --如果目标表是带分区的,如'DW_CUST_ACCT_INFO_PER partition(part_ccbs_sa)'则截取'DW_CUST_ACCT_INFO_PER'
   if instr(p_tar_tab,' ')>0 then
     v_tar_tab:=substr(p_tar_tab,1,instr(p_tar_tab,' ')-1);
   end if;
    --获取约束sql
    VERS_TOOLS.get_tab_constraint_cmd(upper(p_tar_owner), upper(v_tar_tab), cons_sql1, cons_sql2, cons_sql3, cons_sql4);
    --获取索引sql
    VERS_TOOLS.get_tab_index_cmd(upper(p_tar_owner), upper(v_tar_tab), p_parallel, p_tar_idx_tbs, idx_sql1, idx_sql2, idx_sql3, idx_sql4, idx_sql5);
    --获取插入数据sql
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

    --删除所有约束和索引(分区表的时候不做这个操作)
    if trim(v_partition) is null then
       VERS_TOOLS.drop_all_cons_idx(upper(p_tar_owner), upper(v_tar_tab));
    end if;
    --清空表
    if trim(v_partition) is not null then
      EXECUTE IMMEDIATE 'alter table '||p_tar_owner||'.'||v_tar_tab||' truncate '||v_partition;
    else
      EXECUTE IMMEDIATE 'truncate table ' || p_tar_owner || '.' || p_tar_tab;
    end if;
    p_v_sql:=insert_sql;
    --开始插入数据
    EXECUTE IMMEDIATE insert_sql;
    p_row_num:= SQL%ROWCOUNT;
    COMMIT;

    --创建索引
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

      --创建约束
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

  --返回字符串按特定字符分割的数组
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
--将字符串数组按照分隔符转为一行
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
  --生成merge语句的inert部分
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
    --判断表名是否带dblink
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
    --从数据字典匹配列(列名，数据类型两个要素来匹配)
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

    --遍历集合生成insert语句
    FOR item IN 1 .. t_col_table.count LOOP
      v_part_insert := v_part_insert || 't.'||t_col_table(item).t_col || ',';
      v_part_select := v_part_select || 's.'||t_col_table(item).s_col || ',';
    END LOOP;
    v_sql := 'insert(' || rtrim(v_part_insert, ',') || ') values (' ||rtrim(v_part_select, ',') || ')';
    --dbms_output.put_line(v_sql);
  END;

  --生成merge语句的update部分
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
    --判断表名是否带dblink
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
    --从数据字典匹配列(列名，数据类型两个要素来匹配)
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
    --遍历集合生成insert语句
    FOR item IN 1 .. t_col_table.count LOOP
      v_part_update := v_part_update || 't.' || t_col_table(item).t_col ||'=s.' || t_col_table(item).s_col || ',';
    END LOOP;
    v_sql := 'update set ' || rtrim(v_part_update, ',');
    --dbms_output.put_line(v_sql);
  END;

  --生成merge语句的on部分
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

  --生成merge语句
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
    --如果目标表是带分区的,如'DW_TAB partition(part1)'则截取'partition(part1)'
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


   --生成增量数据
  PROCEDURE get_matched_minus_sql(p_tmp_tab VARCHAR2,--存放增量数据临时表
                                 p_src_owner   VARCHAR2,
                                 p_src_tab     VARCHAR2,
                                 p_src_sel_pal   integer,--可选参数,查询源表并发数
                                 p_src_sel_cols  VARCHAR2,----可选参数，查询源表字段
                                 p_sel_where VARCHAR2,
                                 p_tar_sel_pal   integer,--可选参数,查询目标表并发数
                                 p_tar_owner VARCHAR2,
                                 p_tar_tab   VARCHAR2,
                                 p_tar_sel_cols  VARCHAR2,--可选参数，查询目标表字段
                                  p_reject_cols  VARCHAR2,--可选参数，踢掉的字段(一般把ETL_DATE踢掉)
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
    --如果未传入查询字段和插入字段，则认为是同结构表，自动生成SQL语句
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


  --同步结束根据参数表处理目标表
   PROCEDURE handle_tab_end(p_tab   VARCHAR2,--表名
                          p_data_dt varchar2)
                            IS
  v_stepname varchar2(200);
  v_is_part  varchar2(1);--1 是分区表
  v_stat_num integer;
  v_table_owner varchar2(30);--用户名
  v_table_name varchar2(30);--表名
 v_table_space VARCHAR2(30);--表空间名
 v_part_type  VARCHAR2(1);--分区类型D=天M=月
  v_is_analyze VARCHAR2(1);--是否需收集统计信息
 v_last_analy_date DATE;--上次表分析日期
 v_is_move  VARCHAR2(1);--1 move
 v_index_space VARCHAR2(30);--索引空间
 v_is_bak_his  VARCHAR2(1);--是否迁移历史数据 1 迁移
 v_bak_his_table_name VARCHAR2(42);--备份表
 v_bak_his_freq VARCHAR2(1);--迁移历史数据频率
 v_bak_his_keep VARCHAR2(10);--保留数据时长
 v_bak_part_dt_col VARCHAR2(30);--备份(分区)日期
 v_bak_part_dt_exp VARCHAR2(50);--日期表达式
 v_mov_freq VARCHAR2(1);--MOVE频率D=日终M=月末Q=季末Y=年末
 v_is_recreate_tab VARCHAR2(1);--是否需要重建表(MOVE操作，表分析都没有好的效果就设置定期重建)
 v_recreate_tab_freq VARCHAR2(1);--重建频率
 v_data_dt  varchar2(8);
 v_data_dt_d date;
 v_bak_his_end_day date; --备份数据日期
  v_opt_log_id INT;
  v_part_name varchar2(10);--分区名称
  v_is_baktab_index varchar2(1);--备份表是否创建索引
  v_sql varchar2(4000);
  v_sql_clob clob;
    v_bak_owhere_exp varchar2(1000);--其他备份条件
  v_where_exp  varchar2(2000);--备份条件
  v_is_analy_tab varchar2(1);--是否收集统计信息：0-不收集；1-收集,ADD_BY:WUKR@20181023@需求编号2018R1431-优化数据集市日终收集统计信息的需求
  BEGIN
 v_table_name:= upper(p_tab);
 v_data_dt:=p_data_dt;
 v_data_dt_d:=to_date(p_data_dt,'yyyymmdd');
 v_stepname:='检查表是否存在';
 select username into v_table_owner from user_users ;
  select is_usr_tab_exists(v_table_name) into v_stat_num from dual;
  if (v_stat_num <=0) then
  return;
  end if;

  v_stepname:='检查是否有配置参数';
  select count(1) into v_stat_num from ct_tab_param where table_name=v_table_name and status='1';
  if (v_stat_num <=0) then
  return;
  end if;

   v_stepname:='赋值参数';

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
           nvl(a.is_analy_tab,'0') --是否收集统计信息：0-不收集；1-收集,默认不收集，ADD_BY:WUKR@20181023@需求编号2018R1431-优化数据集市日终收集统计信息的需求
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


  v_stepname:='检查是普通表还是分区表';

  select count(1) into v_stat_num  from user_tab_partitions where table_name=v_table_name;
   if(v_stat_num>=1) then
  v_is_part:='1';
  end if;


   v_stepname:='备份历史数据前检查备份表';

   if(v_is_bak_his <>'0' and v_bak_his_table_name is not null) then
            --如果备份日期不是月末日期且表以月分区且分区日期字段与备份字段一致，则调整备份日期为上月末
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
   --判断备份表是否存在，如果不存在则创建新表
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

     --按照数据日期部分备份后，将本表备份的数据删除
  if(v_is_bak_his='1' and v_bak_his_end_day is not null) then
  v_stepname:='备份分区赋值';
    IF v_part_type = 'M' THEN
           v_part_name := 'P_' ||to_char(v_bak_his_end_day,'yyyymm') ;
       ELSE
           v_part_name := 'P_' ||to_char(v_bak_his_end_day,'yyyymmdd') ;
      END IF;
      --20180110 米辉翀 添加and v_part_type is not null 控制非日期分区表采用普通表方式备份
     if(v_is_part='1' and v_part_type is not null) then
       v_stepname:='分区表迁移历史数据';
          for t_tab in (select table_name, partition_name ,high_value,tablespace_name from user_tab_partitions  where table_name=v_table_name and partition_name<=v_part_name)
               loop
               begin
               if( v_bak_his_table_name is not null) then
               --检查备份表是否有分区
               select count(1) into v_stat_num  from user_tab_partitions where table_name=v_bak_his_table_name and partition_name=t_tab.partition_name;
                if(v_stat_num<=0  ) then
                --没有分区则创建分区
                v_sql:='alter table '||v_bak_his_table_name|| ' ADD PARTITION ' ||t_tab.partition_name|| ' values  less than ('||substr(t_tab.high_value,1,4000)||
                     ') TABLESPACE ' || nvl(v_table_space,t_tab.tablespace_name);
               execute immediate v_sql;
                end if;
                --删除备份表数据，支持重跑
                v_sql:='alter table '||v_bak_his_table_name|| ' truncate PARTITION ' ||t_tab.partition_name|| ' Update Global Indexes';
               execute immediate v_sql;
                --插入备份数据
                  VERS_TOOLS.get_matched_inser_sql(v_table_owner, v_table_name,4,null,' partition ('||t_tab.partition_name||')', v_table_owner, v_bak_his_table_name,4,null, v_sql_clob);
                 execute immediate v_sql_clob;
                end if;
                 --删除本表数据
                  select count(1) into v_stat_num  from user_tab_partitions where table_name=v_table_name;
                  if(v_stat_num>=2 ) then
                  v_sql:='alter table '||v_table_name|| ' DROP PARTITION ' ||t_tab.partition_name|| ' Update Global Indexes';
                  --v_sql:='alter table '||v_table_name|| ' DROP PARTITION ' ||t_tab.partition_name;
                  else
                   v_sql:='alter table '||v_table_name|| ' truncate PARTITION ' ||t_tab.partition_name|| ' Update Global Indexes';
               end if;
               execute immediate v_sql;
                commit;
                 --更新备份日期
                update ct_tab_param set LAST_BAK_DATE=sysdate where table_name=v_table_name;
                commit;
                 end;
               end loop;
     else
     v_stepname:='普通表迁移历史数据';
     --删除备份表数据，支持重跑
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
     --插入备份数据
      --20180110 米辉翀 新增其他备份条件
     v_where_exp :=' where '||v_bak_part_dt_col||' <='||v_bak_part_dt_exp;
     if(trim(v_bak_owhere_exp) is not null ) then
     v_where_exp :=v_where_exp||' and '||v_bak_owhere_exp;
     end if;
     VERS_TOOLS.get_matched_inser_sql(v_table_owner, v_table_name,4,null,' where '||v_bak_part_dt_col||' <='||v_bak_part_dt_exp, v_table_owner, v_bak_his_table_name,4,null, v_sql_clob);
     execute immediate v_sql_clob;
     commit;
     end if;
     --删除本表数据
     v_sql:='delete from '||v_table_name||' where '||v_bak_part_dt_col||' <='||v_bak_part_dt_exp;
    execute immediate v_sql;
    commit;
    --更新备份日期
    update ct_tab_param set LAST_BAK_DATE=sysdate where table_name=v_table_name;
    commit;
     end if;
 end if;
   if(v_is_bak_his='2') then
          if( v_bak_his_table_name is not null) then
            v_sql:='truncate table '||v_bak_his_table_name;
            execute immediate v_sql;
            --插入备份数据
             --20180110 米辉翀 新增其他备份条件
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

  --move表
  v_stepname:='move表';
  --调用MOVE表过程
  if(v_is_move='1') then
  move_table(v_table_name,v_table_space,4);
  update ct_tab_param set LAST_MOVE_DATE=sysdate where table_name=v_table_name;
  commit;
  end if;



  v_stepname:='重建表';
  if (v_is_recreate_tab ='1' ) then
  --调用重建表过程
   vers_tools.recre_table(v_table_name);
  end if;

   v_stepname:='检查索引,重建索引';
  --调用重建索引过程
   vers_tools.rebuild_index(v_table_name,v_index_space,4);
  --收集统计信息
  --判断是否需要收集统计信息：0-不收集；1-收集,ADD_BY:WUKR@20181023@需求编号2018R1431-优化数据集市日终收集统计信息的需求
  if (v_is_analy_tab ='1' ) then
    analyze_table(v_table_name,'0',v_stat_num);
    if(v_stat_num=1) then
     update ct_tab_param set LAST_ANALY_DATE=sysdate where table_name=v_table_name;
     commit;
    end if;
  end if;
   --分区表新建分区
     if(v_is_part='1' and v_part_type is not null) then
     --根据是月分区或者日分区赋值分区名称以及新建分区表达式
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
      --判断是否已经有分区，如果没有则新建
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

 --MOVE表
   PROCEDURE move_table(p_tab   VARCHAR2,--表名
                          p_table_space varchar2, --表空间
                         p_parallel integer --并行度
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
  --检查表是否存在
     select is_usr_tab_exists(v_tab) into v_stat_num from dual;
  if (v_stat_num <=0) then
  return;
  end if;
  --检查是否分区表
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
                --重建局部索引
               -- alter table dm_ims_transaction modify partition p_20170820 rebuild unusable local indexes;
                v_sql :='alter table '||t_tab.table_name ||' modify partition '||t_tab.partition_name||' rebuild unusable local indexes ' ;
                --dbms_output.put_line(v_sql);
                execute immediate v_sql;
               end loop;

 else
 --move 普通表
 if(v_table_space is null ) then
 select tablespace_name into v_table_space from user_tables where   table_name=v_tab ;
 end if;
  v_sql :='alter table '||v_tab ||' move tablespace '||v_table_space;
   if(v_lob_cols is not null) then
   v_sql :=v_sql||' lob('||v_lob_cols||') store as ( tablespace '||v_table_space||')';
  end if;
  execute immediate v_sql;
 end if;
   --重建索引
vers_tools.rebuild_index(v_tab,v_table_space,4);
  end move_table;


  --重建索引
   PROCEDURE rebuild_index(p_tab   VARCHAR2,--表名
                         p_index_space varchar2, --索引空间
                          p_parallel integer --并行度
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
  --检查表是否存在
     select is_usr_tab_exists(v_tab) into v_stat_num from dual;
      if (v_stat_num <=0) then
      return;
      end if;
  --重建全局索引
    for t_ind in (select index_name,tablespace_name from user_indexes  where table_name=v_tab and status ='UNUSABLE' )
               loop
               --alter index DM_IMS_TRANSACTION_IDX3 rebuild tablespace DM_TS_ind parallel 4 online nologging;
                v_sql :='alter index '||t_ind.index_name ||' rebuild tablespace '||nvl(v_index_space,t_ind.tablespace_name)||' parallel '||v_parallel||' online nologging' ;
                execute immediate v_sql;
               end loop;
 --重建局部索引
   for t_ind in (select b.index_name,b.partition_name,b.tablespace_name from user_indexes a ,USER_IND_PARTITIONS b  where a.table_name=v_tab and b.status ='UNUSABLE' )
               loop
               -- alter index DM_IMS_TRANSACTION_IDX4 rebuild partition p_20170820 tablespace DM_TS_IND;
                v_sql :='alter index '||t_ind.index_name  ||' rebuild partition '||t_ind.partition_name||' tablespace '||nvl(v_index_space,t_ind.tablespace_name);
                execute immediate v_sql;
                 end loop;


  end rebuild_index;

   PROCEDURE recre_table(p_tab   VARCHAR2--表名
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
     --检查表是否存在
     select is_usr_tab_exists(v_tab) into v_stat_num from dual;
  if (v_stat_num <=0) then
  return;
  end if;
  select username into v_table_owner from user_users ;
  --检查是否分区表
  select count(1) into v_stat_num  from user_tab_partitions where table_name=v_tab;
   if(v_stat_num>=1) then
     v_is_part:='1';
  end if;
   --建新表
  vers_tools.copy_table(v_tab,v_tmp_tbl_name1);
  --插入数据
    if(v_is_part='1' ) then
        --分区表插入数据
          for t_tab in (select table_name, partition_name ,high_value,tablespace_name from user_tab_partitions  where table_name=v_tab )
               loop
                --插入备份数据
                 VERS_TOOLS.get_matched_inser_sql(v_table_owner, v_tab,4,null,' partition ('||t_tab.partition_name||')', v_table_owner, v_tmp_tbl_name1,4,null, v_sql_clob);
                 execute immediate v_sql_clob;
                 COMMIT;
               end loop;
     else
     VERS_TOOLS.get_matched_inser_sql(v_table_owner, v_tab,4,null,null, v_table_owner, v_tmp_tbl_name1,4,null, v_sql_clob);
      execute immediate v_sql_clob;
      COMMIT;
    end if;

  --重建索引
  vers_tools.copy_indexes(v_tab,v_tmp_tbl_name1,'4',null);
  --重命名
  v_sql :='rename ' ||v_tab||' to '||v_tmp_tbl_name2;
  execute immediate v_sql;
   v_sql :='rename ' ||v_tmp_tbl_name1||' to '||v_tab;
  execute immediate v_sql;
  --删除原表
  vers_tools.drop_tab_no_interruption(v_tmp_tbl_name2);

  end recre_table;



      --表分析
    PROCEDURE analyze_table( p_tab   VARCHAR2,--表名
                             is_must varchar2,--是否必须做表分析 1 是 0 否
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
     --检查表是否存在
     select is_usr_tab_exists(v_tab) into v_stat_num from dual;
  if (v_stat_num <=0) then
  return;
  end if;
  select username into v_table_owner from user_users ;
  --检查是否分区表
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
