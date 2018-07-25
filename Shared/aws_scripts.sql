--Restore SQL Server back up files to AMAZON RDS instances.
EXEC msdb.dbo.rds_restore_database
     @restore_db_name = 'MDCRaw',
     @S3_arn_to_restore_from = 'arn:aws:s3:::mars-mdc-database-backup/MDCRaw_backup_2018_06_28_010011_4826757.bak'
