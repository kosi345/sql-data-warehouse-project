/*
create a database and a schema
script purpose: 
      This script creates a datatbase named Datawarehouse after checking if the database already exists.
      If it already exists, the database is dropped and recreated. Also,the script sets up three schemas within the database:
      'bronze','silver' and 'gold'

NOTE:
When the scripts is ran,it will drop the entire 'Datawarehouse' if it exists and all it's data will be permanently deleted.
ensure you have proper backups before running this scripts*/


Use master;
GO
-- drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name ='Datawarehouse')
BEGIN 
ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE Datawarehouse;


-- create the 'Datawarehouse' database
CREATE DATABASE Datawarehouse;
GO 

 use Datawarehouse;

 -- create schemas
 CREATE SCHEMA bronze;

 GO

 CREATE SCHEMA silver;
GO 

 CREATE SCHEMA gold;
