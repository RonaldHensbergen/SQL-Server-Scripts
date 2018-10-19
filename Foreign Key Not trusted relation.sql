/*
Author: Ronald Hensbergen
Create Date: 24-09-2018
Last Updated: 28-09-2018

Objective: To get for all foreign keys that are not trusted:
- the code to check if the referential integrity is impacted
- the code to get the foreign key trusted again

Comment:
A foreign key can get not trusted by disabling the foreign key and when the enabling is not without 
the WITH CHECK option. The problem with non trusted foreign keys is that they won't be used in query plans.
*/
IF OBJECT_ID('tempdb..#temp') IS NOT NULL
    DROP TABLE #temp;
SELECT RANK() OVER (ORDER BY fk.object_id,
                             fk.parent_object_id,
                             fk.name,
                             fk.referenced_object_id
                   ) row_id,
       OBJECT_SCHEMA_NAME(fk.parent_object_id) table_schema_name,
       OBJECT_NAME(fk.parent_object_id) table_name,
       fk.name foreign_key_name,
       OBJECT_NAME(fk.referenced_object_id) referenced_table,
       OBJECT_SCHEMA_NAME(fk.referenced_object_id) referenced_schema_name,
       rc.name referenced_column_name,
       pc.name parent_column_name,
       fk.name foreign_key,
       N'ALTER TABLE ' + OBJECT_SCHEMA_NAME(fk.object_id) + N'.' + OBJECT_NAME(fk.parent_object_id)
       + N' WITH CHECK CHECK CONSTRAINT ' + fk.name sql_stmt
INTO #temp
FROM sys.foreign_keys fk
    INNER JOIN sys.foreign_key_columns fkc
        ON fkc.constraint_object_id = fk.object_id
    INNER JOIN sys.columns pc
        ON fkc.parent_column_id = pc.column_id
           AND fkc.parent_object_id = pc.object_id
    INNER JOIN sys.columns rc
        ON fkc.referenced_column_id = rc.column_id
           AND fkc.referenced_object_id = rc.object_id
WHERE fk.is_disabled = 0
      AND fk.is_not_trusted = 1
ORDER BY table_schema_name,
         table_name;

SELECT t2.row_id,
       t2.sql_stmt,
       N'SELECT ''' + t2.foreign_key + ''', COUNT(1) FROM ' + t2.table_schema_name + '.' + t2.table_name
       + ' T1 LEFT OUTER JOIN ' + t2.referenced_schema_name + '.' + t2.referenced_table + ' T2 ON '
       + SUBSTRING(
         (
             SELECT ' AND T1.' + t1.parent_column_name + ' = T2.' + t1.referenced_column_name AS [text()]
             FROM #temp t1
             WHERE t1.row_id = t2.row_id
             ORDER BY t1.row_id
             FOR XML PATH('')
         ),
         5,
         1000
                  ) + ' WHERE T2.' + MIN(t2.referenced_column_name) + ' IS NULL' query_text
FROM #temp t2
GROUP BY t2.table_schema_name,
         t2.table_name,
         t2.foreign_key_name,
         t2.referenced_schema_name,
         t2.referenced_table,
         t2.sql_stmt,
         t2.row_id,
         t2.foreign_key
ORDER BY t2.row_id;
