USE master;
GO

-- DROP DATABASE IF EXISTS DataWarehouse;
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	DROP DATABASE DataWarehouse;
END;
GO

-- Create a new database named DataWarehouse
CREATE DATABASE DataWarehouse;
GO

-- Switch to the newly created database
USE DataWarehouse;
GO

-- Create three schemas: bronze, silver, and gold
CREATE SCHEMA bronze;
GO -- separates batches of SQL statements to be executed together 

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
