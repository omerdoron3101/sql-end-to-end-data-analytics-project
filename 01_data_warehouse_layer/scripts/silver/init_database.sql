/* =============================================================================
   Create Database and Schemas
   ============================================================================= 

   Script Purpose:
       Initialize the Data Warehouse environment by creating the required
       database and schemas (bronze, silver, gold) following Medallion Architecture.

   ⚠️  WARNING:
       Running this script will permanently DROP and recreate the entire 'DataWarehouse'
       database. All existing data, tables, views, and stored procedures will be lost.
	   Proceed with caution ans ensure you have proper bachups before running this script.
   ============================================================================= */

USE master;
GO

-- Drop existing database if present (optional for controlled environments)
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create and switch to new Data Warehouse
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO

-- Create Medallion Architecture schemas
CREATE SCHEMA bronze;  -- Raw data
CREATE SCHEMA silver;  -- Cleansed and standardized data
CREATE SCHEMA gold;    -- Business-ready analytical data
