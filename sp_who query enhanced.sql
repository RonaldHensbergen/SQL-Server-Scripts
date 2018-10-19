/*
Author: Ronald Hensbergen
Create Date: 31-05-2018
Last Updated: 28-09-2018

Objective: To have all the information you get from sp_who(2), but with extra information like:
- the currently running query
- query plan
- object (stored procedure or function) where the query is called from
- transaction information
- etc

Comment:
The code for isolating the current running command I took from 
https://www.mssqltips.com/sqlservertip/1811/how-to-isolate-the-current-running-commands-in-sql-server/
The code for the transaction isolation level I took from 
https://blogs.msdn.microsoft.com/sqlcan/2012/05/24/a-microsoft-sql-server-dmvdmf-cheat-sheet/
*/

DECLARE @ActiveRequest BIT = 1; --1: Only show SPIDs with active requests (status not sleeping and no open transactions), 0: show all SPIDs
DECLARE @SQL NVARCHAR(MAX);
SET @SQL
    = N'
SELECT es.session_id AS SPID,
       r.blocking_session_id,
	   es.host_name,
       DB_NAME(r.database_id) database_name,
       es.login_name,
       es.original_login_name,
       r.command,
       CASE es.status
           WHEN ''sleeping'' THEN
               LOWER(es.STATUS)
           ELSE
               UPPER(es.STATUS)
       END status,
       OBJECT_SCHEMA_NAME(est.objectid, r.database_id) + ''.'' + OBJECT_NAME(est.objectid, r.database_id) object_name,
       DATEDIFF(SECOND, tat.transaction_begin_time, GETDATE()) transaction_in_seconds,
       est.text AS Query,
	   r.query_hash,
       eqp.query_plan,
       CASE
           WHEN r.[statement_start_offset] > 0 THEN
               --The start of the active command is not at the beginning of the full command text 
               CASE r.[statement_end_offset]
                   WHEN -1 THEN
                       --The end of the full command is also the end of the active statement 
                       SUBSTRING(est.text, (r.[statement_start_offset] / 2) + 1, 2147483647)
                   ELSE
                       --The end of the active statement is not at the end of the full command 
                       SUBSTRING(
                                    est.text,
                                    (r.[statement_start_offset] / 2) + 1,
                                    (r.[statement_end_offset] - r.[statement_start_offset]) / 2
                                )
               END
           ELSE
               --1st part of full command is running 
               CASE r.[statement_end_offset]
                   WHEN -1 THEN
                       --The end of the full command is also the end of the active statement 
                       RTRIM(LTRIM(est.[text]))
                   ELSE
                       --The end of the active statement is not at the end of the full command 
                       LEFT(est.text, (r.[statement_end_offset] / 2) + 1)
               END
       END AS [executing statement],
       CAST(r.start_time AS DATETIME2(0)) start_time,
       r.total_elapsed_time / 60000 execution_in_minutes,
       CAST(GETDATE() AS DATETIME2(0)) [current_time],
       CAST(tat.transaction_begin_time AS DATETIME2(0)) transaction_begin_time,
	   CASE es.transaction_isolation_level
		 WHEN 0 THEN ''Unspecified''
		 WHEN 1 THEN ''Read Uncomitted''
		 WHEN 2 THEN ''Read Committed''
		 WHEN 3 THEN ''Repeatable''
		 WHEN 4 THEN ''Serializable''
		 WHEN 5 THEN ''Snapshot''
		 END AS transaction_isolation_level,
       r.cpu_time,
       r.reads reads_pages,
       r.logical_reads,
       r.writes writes_pages,
       r.open_transaction_count,
       r.open_resultset_count,
       r.wait_resource,
       r.wait_time,
       r.wait_type,
       r.last_wait_type';
IF CAST(SERVERPROPERTY('ProductMajorVersion') AS SMALLINT) >= 13
    SET @SQL = @SQL + N'
	   ,
       r.dop';
SET @SQL
    = @SQL
      + N'
FROM sys.dm_exec_connections ec
    LEFT OUTER JOIN sys.dm_exec_sessions es
        ON es.session_id = ec.session_id
    LEFT OUTER JOIN sys.dm_exec_requests r
        ON r.connection_id = ec.connection_id
    LEFT OUTER JOIN sys.dm_tran_active_transactions tat
        ON tat.transaction_id = r.transaction_id
    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) est
    OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) eqp
WHERE es.session_id <> @@SPID';
IF @ActiveRequest = 1
BEGIN
    SET @SQL = @SQL + N'
	AND
	(
		es.status <> ''sleeping''
		OR r.open_transaction_count > 0';
END;
SET @SQL = @SQL + N'
	)';
SET @SQL = @SQL + N'
ORDER BY SPID;';
PRINT @SQL;
EXEC sp_executesql @statement = @SQL;