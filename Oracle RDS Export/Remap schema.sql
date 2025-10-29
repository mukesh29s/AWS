----- Export the schema
DECLARE
  v_hdnl NUMBER;
BEGIN
  v_hdnl := DBMS_DATAPUMP.OPEN(
    operation => 'EXPORT', 
    job_mode  => 'SCHEMA', 
    job_name  => null
  );
  DBMS_DATAPUMP.ADD_FILE( 
    handle    => v_hdnl, 
    filename  => 'Stage-ExportOF_TestDBA28102025_01.dmp',
    directory => 'DATA_PUMP_DIR', 
    filetype  => dbms_datapump.ku$_file_type_dump_file
  );
  DBMS_DATAPUMP.ADD_FILE( 
    handle    => v_hdnl, 
    filename  => 'Stage-ExportOF_TestDBA28102025_01.log',
    directory => 'DATA_PUMP_DIR', 
    filetype  => dbms_datapump.ku$_file_type_log_file
  );
  DBMS_DATAPUMP.METADATA_FILTER(v_hdnl,'SCHEMA_EXPR','IN (''TEST_DBA'')');
  DBMS_DATAPUMP.METADATA_FILTER(
    v_hdnl,
    'EXCLUDE_NAME_EXPR',
    q'[IN (SELECT NAME FROM SYS.OBJ$ 
           WHERE TYPE# IN (66,67,74,79,59,62,46) 
           AND OWNER# IN 
             (SELECT USER# FROM SYS.USER$ 
              WHERE NAME IN ('RDSADMIN','SYS','SYSTEM','RDS_DATAGUARD','RDSSEC')
             )
          )
    ]',
    'PROCOBJ'
  );
  DBMS_DATAPUMP.START_JOB(v_hdnl);
END;
/

---- Monitor the log while EXPDP or IMPDP is running. 
set pagesize 1000 linesize 200
SELECT text 
	FROM table(rdsadmin.rds_file_util.read_text_file(
    p_directory => 'DATA_PUMP_DIR',
    p_filename  => 'PROD-ImportOF_TCM_GREEN29102025_01.log'));



----Create a user before starting the import. (Optional)                                                                                              

create user remap_test_dba identified by "B15Nin7J9V1V" default tablespace users;
grant connect to remap_test_dba;
grant dba to remap_test_dba;

----- Run the IMPPORT job using REMAP option
DECLARE
  v_hdnl NUMBER;
BEGIN
  v_hdnl := DBMS_DATAPUMP.OPEN(
    operation => 'IMPORT',
    job_mode  => 'SCHEMA',
    job_name  => NULL);
  DBMS_DATAPUMP.ADD_FILE(
    handle    => v_hdnl,
    filename  => 'PROD-ExportOF_TCM_GREEN29102025.dmp',
    directory => 'DATA_PUMP_DIR',
    filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE);
  DBMS_DATAPUMP.ADD_FILE(
    handle    => v_hdnl,
    filename  => 'PROD-ImportOF_TCM_GREEN29102025_01.log',
    directory => 'DATA_PUMP_DIR',
    filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);
  DBMS_DATAPUMP.METADATA_REMAP(
    handle    => v_hdnl,
    name      => 'REMAP_SCHEMA',
    old_value => 'TCM_GREEN',
    value     => 'TCM_UPGRADETEST');
  DBMS_DATAPUMP.START_JOB(v_hdnl);
END;
/