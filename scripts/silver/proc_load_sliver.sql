/*
---------------------------------------------------------------
stored procedure: load silver layer( bronze=>silver)
---------------------------------------------------------------
Script purpose:
  This stored procedure performs the ETL(extract,transform, load) process to
  populate the 'silver' schema tables formnthe bronze schema.
Actions peroformed:
-Truncates the silver tables.
-Inserts transformed and cleansed data from bronze into Silver tabels
USAGE EXAMPLE:
EXEC  silver.load_silver
--------------------------------------------------------------------
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	
 DECLARE @start_time DATETIME,@end_time DATETIME,@start_batch_time DATETIME,@end_batch_time DATETIME
	BEGIN TRY
	SET @start_batch_time=GETDATE()
	
		PRINT'=======================================';
		PRINT'LOADING SILVER LAYER';
		PRINT'=======================================';

		PRINT'---------------------------------------';
		PRINT'LOADING CRM TABLES';
		PRINT'---------------------------------------';
		SET @start_time=GETDATE()
		PRINT'Truncating table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT'Inserting data into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		SELECT 
			  cst_id,
			  cst_key,
			  TRIM(cst_firstname)  cst_firstname,
			  TRIM( cst_lastname)  cst_lastname,
			  CASE WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
				   WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'	
				   ELSE 'N/A'
				   END    cst_marital_status,
			  CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
				   WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
				   ELSE 'N/A' 
				   END cst_gndr,
			  cst_create_date
  
		FROM(

			SELECT *,
			ROW_NUMBER()OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id is not null
		 )t WHERE flag_last=1 ;
		 SET @end_time=GETDATE();
		 PRINT'THE DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		 PRINT'========================================================================================'

		
		SET @start_time=GETDATE()
		PRINT'Truncating table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT'Inserting data into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id ,
			cat_id ,
			prd_key,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt ,
			prd_end_dt 
		)

		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,-- extract Caterory ID
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,-- extract Product key
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 's' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
			END  AS prd_line,-- Map Product line code to descriptive values
			CAST(prd_start_dt AS DATE) prd_start_dt,
			CAST(
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 
			AS DATE
			) AS prd_end_dt -- Calculate end dat eas one day before the next start date
		FROM bronze.crm_prd_info;
		SET @end_time=GETDATE();
		PRINT'THE DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		PRINT'========================================================================================'



		SET @start_time=GETDATE()
		PRINT'Truncating table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT'Inserting data into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price


		)
			SELECT
				sls_ord_num ,
				sls_prd_key ,
				sls_cust_id ,
				CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
				CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt ) != 8 THEN NULL
						ELSE CAST(CAST(sls_ship_dt  AS VARCHAR) AS DATE)
				END AS sls_ship_dt ,
				CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt ) != 8 THEN NULL
						ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
				CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
						THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
						END AS sls_sales, -- Recalculate sales if original value is ,issing or incorrect
				sls_quantity,
				CASE WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
					END AS sls_price -- Derive price if original value is invalid
			FROM bronze.crm_sales_details;
		SET @end_time=GETDATE();
		PRINT'THE DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		PRINT'========================================================================================'


		
		PRINT'---------------------------------------';
		PRINT'LOADING THE ERP TABLES';
		PRINT'---------------------------------------';
		
		
		SET @start_time=GETDATE()
		PRINT'Truncating table: silver.erp_CUST_AZ12'
		TRUNCATE TABLE silver.erp_CUST_AZ12
		PRINT'Inserting data into: silver.erp_CUST_AZ12'
		INSERT INTO silver.erp_CUST_AZ12(cid,bdate,gen)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LEN(cid))
				ELSE cid
				END AS cid,
		CASE WHEN bdate> GETDATE() THEN NULL
				ELSE bdate
				END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'male'
				 ELSE 'n/a'
				 END AS gen
		FROM bronze.erp_CUST_AZ12
		SET @end_time=GETDATE();
		PRINT'THE DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		PRINT'========================================================================================'




		SET @start_time=GETDATE()
		PRINT'Truncating table: silver.erp_LOC_A101'
		TRUNCATE TABLE silver.erp_LOC_A101
		PRINT'Inserting data into: silver.erp_LOC_A101'
		INSERT INTO  silver.erp_LOC_A101(cid,cntry)
		SELECT 
		REPLACE(cid,'-','') AS cid,
		CASE WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United states'
			 WHEN UPPER(TRIM(cntry)) ='DE'  THEN 'Germany'
			 WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = ' ' or  UPPER(TRIM(cntry)) = ''THEN 'n/a'
			 ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_LOC_A101
		SET @end_time=GETDATE();
		PRINT'THE DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		PRINT'========================================================================================'




		SET @start_time=GETDATE()
		PRINT'Truncating table: silver.erp_PX_CAT_G1V2'
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2
		PRINT'Inserting data into: silver.erp_PX_CAT_G1V2'
		INSERT INTO silver.erp_PX_CAT_G1V2(id,cat,subcat, maintenance )
			SELECT 
			id,
			cat,
			subcat,
			maintenance
			FROM bronze.erp_PX_CAT_G1V2
		SET @end_time=GETDATE();
		PRINT'THE DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		PRINT'========================================================================================'

		SET @end_batch_time=GETDATE();
		PRINT'---------------------------LOADING OF SILVER LAYER COMPLETED------------------------------'
		PRINT'THE whole duration of silver layer: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		PRINT'========================================================================================'
	 END TRY

	 BEGIN CATCH
	 PRINT'ERROR OCCURED WHILE TRYING TO LOAD THE SILVER LAYER'
	 PRINT'ERROR MESSAGE: '+ERROR_MESSAGE();
	 PRINT'ERROR LINE: '+CAST(ERROR_LINE() AS NVARCHAR);
	 END CATCH
END 



