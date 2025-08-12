/*
=====================================================
stored procedure:Load Bronze Layer(source-> Bronze)
=====================================================
purpose:
  Ths stored procedure load data into the ' bronze ' schema from external CSV files.
  It performs this actions:
  - Firstly it will truncate the bronze tables before loading data i.e clearing any data in the table that existed before.
  - then then uses the 'Bulk insert' command to load data from the csv files to bronze tables.
Usage Example:
EXEC bronze.load_bronze;
===================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT'=======================================';
		PRINT'LOADING BRONZE LAYER';
		PRINT'=======================================';

		PRINT'---------------------------------------';
		PRINT'LOADING CRM TABLES';
		PRINT'---------------------------------------';

		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>>INSERTING DATA INTO: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT'Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT'---------------------------------------------------------'

		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'>>INSERTING DATA INTO: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds'
		PRINT'--------------------------------------------------------'

		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'>>INSERTING DATA INTO: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds'
		PRINT'--------------------------------------------------------'


		PRINT'---------------------------------------';
		PRINT'LOADING THE ERP TABLES';
		PRINT'---------------------------------------';

		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.erp_CUST_AZ12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT'>>INSERTING DATA INTO: bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds'
		PRINT'--------------------------------------------------------'


		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.erp_LOC_A101'
		TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT'>>INSERTING DATA INTO: bronze.erp_LOC_A101'
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds'
		PRINT'--------------------------------------------------------'


		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE:  bronze.erp_PX_CAT_G1V2'
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

	
		PRINT'>>INSERTING DATA INTO:  bronze.erp_PX_CAT_G1V2'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds';
		PRINT'--------------------------------------------------------'

		SET @batch_end_time=GETDATE();
		PRINT'=======================================================';
		PRINT'Loading for the bronze layer is completed';
		PRINT'Load duration for the whole batch: '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT'============================================================'
	END TRY
	BEGIN CATCH
	PRINT'================================================';
	PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT'Error message'+ERROR_MESSAGE();
	PRINT'Error message'+CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT'================================================';
	END CATCH
	
END
