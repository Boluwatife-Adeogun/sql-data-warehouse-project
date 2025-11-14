/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    SELECT bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE FUNCTION bronze.load_bronze()
RETURNS void AS $$
BEGIN
	
	TRUNCATE TABLE bronze.crm_cust_info;
	
	--COPY bronze.crm_cust_info
	--FROM 'C:/pg_uploads/cust_info.csv'
	--DELIMITER ','
	--CSV HEADER;
	
	TRUNCATE TABLE bronze.crm_prd_info;
	
	--COPY bronze.crm_prd_info
	--FROM 'C:/pg_uploads/prd_info.csv'
	--DELIMITER ','
	--CSV HEADER;
	
	TRUNCATE TABLE bronze.crm_sales_details;
	
	--COPY bronze.crm_sales_details
	--FROM 'C:\pg_uploads\sales_details.csv'
	--DELIMITER ','
	--CSV HEADER;
	
	TRUNCATE TABLE bronze.erp_cust_az12;
	
	--COPY bronze.erp_cust_az12
	--FROM 'C:\pg_uploads\CUST_AZ12.csv'
	--DELIMITER ','
	--CSV HEADER;
	
	TRUNCATE TABLE bronze.erp_loc_a101;
	
	--COPY bronze.erp_loc_a101
	--FROM 'C:\pg_uploads\LOC_A101.csv'
	--DELIMITER ','
	--CSV HEADER;
	
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	--COPY bronze.erp_px_cat_g1v2
	--FROM 'C:\pg_uploads\PX_CAT_G1V2.csv'
	--DELIMITER ','
	--CSV HEADER;
END;
$$ LANGUAGE plpgsql;
