IF DB_ID('ConversionIngestion') IS NULL
BEGIN
    CREATE DATABASE ConversionIngestion;
END;
GO

USE ConversionIngestion;
GO

IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = 'ingestion_user')
BEGIN
    CREATE LOGIN ingestion_user WITH PASSWORD = 'ILuvMigration<>05', CHECK_POLICY = ON;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'ingestion_user')
BEGIN
    CREATE USER ingestion_user FOR LOGIN ingestion_user;
END;
GO

ALTER ROLE db_owner ADD MEMBER ingestion_user;
GO
