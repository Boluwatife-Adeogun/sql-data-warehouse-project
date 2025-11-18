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

-- ===========================================
-- Function: bronze.load_bronze()
-- Purpose:  Load data into Bronze layer tables
--           with truncate and copy commands,
--           and log load history with durations.
-- ===========================================
*/

CREATE OR REPLACE FUNCTION bronze.load_bronze()
RETURNS void AS
$$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    -- Log start of Bronze load
    INSERT INTO bronze_logs.load_history(process, message)
    VALUES ('BRONZE_LOAD', 'Starting Bronze layer load process.');

    -- Load crm_cust_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info FROM 'C:/pg_uploads/cust_info.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();

    INSERT INTO bronze_logs.load_history(process, table_name, message, duration_seconds)
    VALUES ('BRONZE_LOAD', 'crm_cust_info', 'Loaded successfully', EXTRACT(EPOCH FROM (end_time - start_time)));

    -- Load crm_prd_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info FROM 'C:/pg_uploads/prd_info.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();

    INSERT INTO bronze_logs.load_history(process, table_name, message, duration_seconds)
    VALUES ('BRONZE_LOAD', 'crm_prd_info', 'Loaded successfully', EXTRACT(EPOCH FROM (end_time - start_time)));

    -- Load crm_sales_details
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details FROM 'C:/pg_uploads/sales_details.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();

    INSERT INTO bronze_logs.load_history(process, table_name, message, duration_seconds)
    VALUES ('BRONZE_LOAD', 'crm_sales_details', 'Loaded successfully', EXTRACT(EPOCH FROM (end_time - start_time)));

    -- Load erp_cust_az12
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12 FROM 'C:/pg_uploads/cust_az12.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();

    INSERT INTO bronze_logs.load_history(process, table_name, message, duration_seconds)
    VALUES ('BRONZE_LOAD', 'erp_cust_az12', 'Loaded successfully', EXTRACT(EPOCH FROM (end_time - start_time)));

    -- Load erp_loc_a101
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101 FROM 'C:/pg_uploads/loc_a101.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();

    INSERT INTO bronze_logs.load_history(process, table_name, message, duration_seconds)
    VALUES ('BRONZE_LOAD', 'erp_loc_a101', 'Loaded successfully', EXTRACT(EPOCH FROM (end_time - start_time)));

    -- Load erp_px_cat_g1v2
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2 FROM 'C:/pg_uploads/px_cat_g1v2.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();

    INSERT INTO bronze_logs.load_history(process, table_name, message, duration_seconds)
    VALUES ('BRONZE_LOAD', 'erp_px_cat_g1v2', 'Loaded successfully', EXTRACT(EPOCH FROM (end_time - start_time)));

    -- Log completion of Bronze load
    INSERT INTO bronze_logs.load_history(process, message)
    VALUES ('BRONZE_LOAD', 'Completed Bronze layer load process.');

EXCEPTION WHEN OTHERS THEN
    -- Log any errors
    INSERT INTO bronze_logs.load_history(process, message)
    VALUES ('BRONZE_LOAD', 'ERROR: ' || SQLERRM);
    RAISE;
END;
$$
LANGUAGE plpgsql;
