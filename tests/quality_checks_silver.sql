/*
--------------------------------------------------------------------------
Quality checks
--------------------------------------------------------------------------
Script Purpose:
  This script here performs various quality checks for data consistency,accuracy and standardization across the 'silver' schema.
  It includes checks for:
  -nulls or Duplicates in primary key
  -unwanted spaces in string fields
  -Data standardization and consistency
  -invalid date ranges and orders
  -Data consistency between related fields

NOTE:
  - run these checks after data loading Silver layer
  - Investigate and resolve any issues found the checks
---------------------------------------------------------------------------
*/


--  ------------------------------------------------------------------
-- checking 'silver.crm_cust_info'
-- -------------------------------------------------------------------
-- check for nulls or Duplicates in primary key
-- no result expected
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL;

-- check for unwanted Spaces
-- Expection: no results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- data standardization and consistency
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

--  ------------------------------------------------------------------
-- checking 'silver.crm_prd_info'
-- -------------------------------------------------------------------

-- check for nulls or Duplicates in primary key
-- no result expected
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL;

-- check for unwanted Spaces
-- Expection: no results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- check for nulls or negative numbers 
-- no result expected
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

-- data standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt< prd_start_dt
  
--  ------------------------------------------------------------------
-- checking 'silver.crm_sales_details'
-- -------------------------------------------------------------------

-- check for invalid dates 
SELECT 
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt>sls_due_dt  

  
--  ------------------------------------------------------------------
-- checking 'silver.erp_CUST_AZ12'
-- -------------------------------------------------------------------
-- Identity out of range Dates
SELECT DISTINCT 
bdate
FROM silver.erp_CUST_AZ12
WHERE bdate>GETDATE()

-- Data consistency and standardization
SELECT DISTINCT gen
FROM silver.erp_CUST_AZ12

--  ------------------------------------------------------------------
-- checking 'silver.erp_LOC_A101'
-- -------------------------------------------------------------------

-- 
SELECT 
cid
FROM silver.erp_LOC_A101

-- Data standardzation & consistency
SELECT DISTINCT cntry
FROM silver.erp_LOC_A101
  
--  ------------------------------------------------------------------
-- checking 'silver.erp_PX_CAT_G1V2'
-- -------------------------------------------------------------------
-- check for unwanted spaces
SELECT * FROM silver.erp_PX_CAT_G1V2
WHERE cat != TRIM(cat) or subcat != TRIM(subcat)

-- data consistency and standardization
SELECT DISTINCT 
MAINTENANCE
FROM silver.erp_PX_CAT_G1V2
