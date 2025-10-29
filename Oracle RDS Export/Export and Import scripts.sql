-- ============================================================================
-- Oracle Data Pump Export/Import Script for Schema Backup and Restore
-- Target Schema: TCM_GREEN
-- Environment: Oracle RDS
-- ============================================================================

-- ============================================================================
-- SECTION 1: PRE-EXPORT CHECKS
-- ============================================================================

-- Set SQL*Plus formatting
SET PAGESIZE 400 LINESIZE 400
COL owner FORMAT A10
COL directory_name FORMAT A40
COL directory_path FORMAT A80

-- Verify DATA_PUMP_DIR directory path
SELECT directory_path 
FROM dba_directories 
WHERE directory_name = 'DATA_PUMP_DIR';


-- ============================================================================
-- SECTION 2: EXPORT SCHEMA
-- ============================================================================

DECLARE
  v_hdnl NUMBER;
BEGIN
  -- Open Data Pump export job
  v_hdnl := DBMS_DATAPUMP.OPEN(
    operation => 'EXPORT', 
    job_mode  => 'SCHEMA', 
    job_name  => NULL
  );
  
  -- Add dump file
  DBMS_DATAPUMP.ADD_FILE( 
    handle    => v_hdnl, 
    filename  => 'UAT_ExportOF_TCM_GREEN_24102025_01.dmp',
    directory => 'DATA_PUMP_DIR', 
    filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE
  );
  
  -- Add log file
  DBMS_DATAPUMP.ADD_FILE( 
    handle    => v_hdnl, 
    filename  => 'UAT_ExportOF_TCM_GREEN_24102025_01.log',
    directory => 'DATA_PUMP_DIR', 
    filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE
  );
  
  -- Filter to export only TCM_GREEN schema
  DBMS_DATAPUMP.METADATA_FILTER(
    v_hdnl,
    'SCHEMA_EXPR',
    'IN (''TCM_GREEN'')'
  );
  
  -- Exclude system-owned objects
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
  
  -- Start the export job
  DBMS_DATAPUMP.START_JOB(v_hdnl);
  
  DBMS_OUTPUT.PUT_LINE('Export job started successfully.');
END;
/


-- ============================================================================
-- SECTION 3: MONITOR EXPORT PROGRESS
-- ============================================================================

SET PAGESIZE 1000 LINESIZE 200

-- Read and display log file contents
SELECT text 
FROM TABLE(rdsadmin.rds_file_util.read_text_file(
    p_directory => 'DATA_PUMP_DIR',
    p_filename  => 'UAT_ExportOF_TCM_GREEN_24102025_01.log'
));


-- ============================================================================
-- SECTION 4: UPLOAD TO S3 (Optional but Recommended)
-- ============================================================================

-- Upload dump file to S3 for off-instance backup
SELECT rdsadmin.rdsadmin_s3_tasks.upload_to_s3(
    p_bucket_name    => 'tcm-uat-rds-backup',
    p_prefix         => 'UAT_ExportOF_TCM_GREEN_24102025_01.dmp', 
    p_s3_prefix      => '', 
    p_directory_name => 'DATA_PUMP_DIR'
) AS TASK_ID 
FROM DUAL;

-- Check upload task status
SELECT text 
FROM TABLE(rdsadmin.rds_file_util.read_text_file(
    p_directory => 'BDUMP',
    p_filename  => 'dbtask-<TASK_ID>.log'
));


-- ============================================================================
-- SECTION 5: PRE-IMPORT - CHECK ACTIVE SESSIONS
-- ============================================================================

COL username FORMAT A30

-- Check for active sessions on TCM_GREEN schema
SELECT sid, serial#, username, status, program
FROM v$session 
WHERE username = 'TCM_GREEN'
ORDER BY sid;


-- ============================================================================
-- SECTION 6: DISCONNECT ACTIVE SESSIONS
-- ============================================================================

SET PAGESIZE 900

-- Generate disconnect statements for all active sessions
SELECT 'ALTER SYSTEM DISCONNECT SESSION ''' || sid || ',' || serial# || ''' IMMEDIATE;' 
FROM v$session 
WHERE username = 'TCM_GREEN';

-- Execute the generated statements manually or use dynamic SQL


-- ============================================================================
-- SECTION 7: DROP EXISTING SCHEMA
-- ============================================================================

-- WARNING: This will permanently delete all objects in TCM_GREEN schema
DROP USER TCM_GREEN CASCADE;
COMMIT;


-- ============================================================================
-- SECTION 8: IMPORT SCHEMA
-- ============================================================================

DECLARE
  v_hdnl NUMBER;
BEGIN
  -- Open Data Pump import job
  v_hdnl := DBMS_DATAPUMP.OPEN( 
    operation => 'IMPORT', 
    job_mode  => 'SCHEMA', 
    job_name  => NULL
  );
  
  -- Add dump file to import from
  DBMS_DATAPUMP.ADD_FILE( 
    handle    => v_hdnl, 
    filename  => 'UAT_ExportOF_TCM_GREEN_24102025_01.dmp', 
    directory => 'DATA_PUMP_DIR', 
    filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE
  );
  
  -- Add log file for import
  DBMS_DATAPUMP.ADD_FILE( 
    handle    => v_hdnl, 
    filename  => 'UAT_ImportOF_TCM_GREEN_24102025_01.log', 
    directory => 'DATA_PUMP_DIR', 
    filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE
  );
  
  -- Filter to import only TCM_GREEN schema
  DBMS_DATAPUMP.METADATA_FILTER(
    v_hdnl,
    'SCHEMA_EXPR',
    'IN (''TCM_GREEN'')'
  );
  
  -- Start the import job
  DBMS_DATAPUMP.START_JOB(v_hdnl);
  
  DBMS_OUTPUT.PUT_LINE('Import job started successfully.');
END;
/


-- ============================================================================
-- SECTION 9: MONITOR IMPORT PROGRESS
-- ============================================================================

-- Read and display import log file contents
SELECT text
FROM TABLE(rdsadmin.rds_file_util.read_text_file(
    p_directory => 'DATA_PUMP_DIR',
    p_filename  => 'UAT_ImportOF_TCM_GREEN_24102025_01.log'
));


-- ============================================================================
-- SECTION 10: POST-IMPORT - RECOMPILE OBJECTS
-- ============================================================================

-- Recompile all invalid objects in the schema
EXEC UTL_RECOMP.RECOMP_PARALLEL(NULL, 'TCM_GREEN');

-- Alternative: Recompile schema objects
BEGIN
  DBMS_UTILITY.COMPILE_SCHEMA(
    schema => 'TCM_GREEN',
    compile_all => FALSE  -- FALSE = only invalid objects
  );
END;
/


-- ============================================================================
-- SECTION 11: POST-IMPORT - GATHER STATISTICS
-- ============================================================================

-- Gather schema statistics for optimal performance
EXEC DBMS_STATS.GATHER_SCHEMA_STATS( -
    ownname          => 'TCM_GREEN', -
    options          => 'GATHER AUTO', -
    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, -
    degree           => DBMS_STATS.DEFAULT_DEGREE, -
    cascade          => TRUE -
);


-- ============================================================================
-- SECTION 12: POST-IMPORT VALIDATION
-- ============================================================================

-- Check for invalid objects
SELECT object_type, object_name, status
FROM dba_objects
WHERE owner = 'TCM_GREEN'
AND status = 'INVALID'
ORDER BY object_type, object_name;

-- Count objects by type
SELECT object_type, COUNT(*) as object_count
FROM dba_objects
WHERE owner = 'TCM_GREEN'
GROUP BY object_type
ORDER BY object_type;

-- Verify table row counts (compare with pre-export counts)
SELECT table_name, num_rows
FROM dba_tables
WHERE owner = 'TCM_GREEN'
ORDER BY table_name;


-- ============================================================================
-- NOTES AND BEST PRACTICES
-- ============================================================================
/*
1. FILENAME CONSISTENCY: Use consistent naming convention (underscores instead 
   of hyphens) for better compatibility across systems.

2. ERROR HANDLING: The current script lacks error handling. Consider adding 
   exception blocks in production environments.

3. S3 UPLOAD: Always verify S3 upload completion before deleting local dumps.

4. BACKUP VERIFICATION: Test restore process in non-production environment first.

5. MONITORING: Check DBA_DATAPUMP_JOBS view for job status:
   SELECT * FROM DBA_DATAPUMP_JOBS WHERE STATE != 'NOT RUNNING';

6. DISK SPACE: Verify sufficient space before export:
   SELECT * FROM DBA_FREE_SPACE WHERE TABLESPACE_NAME = 'DATA_PUMP_DIR';

7. PARALLEL OPERATIONS: For large schemas, consider adding PARALLEL parameter 
   to Data Pump operations.

8. TABLESPACE MAPPING: If importing to different tablespaces, use 
   REMAP_TABLESPACE in the import job.
*/