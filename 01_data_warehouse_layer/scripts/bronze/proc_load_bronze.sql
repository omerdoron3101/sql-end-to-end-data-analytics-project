/* =============================================================================
   Stored Procedure: Load Bronze Layer (Sourse -> Bronze)
   ============================================================================= 

   Script Purpose:
       This stored procedure automates the data ingestion process 
       from raw source files (CRM & ERP) into the Bronze layer tables
       of the Data Warehouse. The Bronze layer acts as the initial
       data landing zone, storing untransformed data exactly as
       received from source systems.

   ⚠️ Warning:
       Executing this procedure will TRUNCATE and RELOAD all Bronze tables.
       Any existing data in these tables will be permanently deleted.

   Parameters:
       None.

   Usage Example:
       EXEC bronze.load_bronze;

   ============================================================================= */

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================================================';

        /* ----------------------------------------------------------------------
           SECTION 1: Load CRM Source Data
           ----------------------------------------------------------------------
           This section loads raw data from the CRM source files into the 
           corresponding Bronze tables. Each table is truncated before reloading 
           to maintain consistency with the latest available source data.
        ---------------------------------------------------------------------- */

		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------------------------';

		-- CRM Customer Information
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\User\Desktop\Project-SQL-Data-Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------'

		-- CRM Product Information
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\User\Desktop\Project-SQL-Data-Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------'

		-- CRM Sales Details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\User\Desktop\Project-SQL-Data-Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------'

		/* ----------------------------------------------------------------------
           SECTION 2: Load ERP Source Data
           ----------------------------------------------------------------------
           This section ingests raw data from ERP source systems into
           Bronze layer tables. The process is identical to CRM ingestion.
        ---------------------------------------------------------------------- */

		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------------------------------------';
		
		-- ERP Customer Data
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\User\Desktop\Project-SQL-Data-Warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------'

		-- ERP Location Data
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\User\Desktop\Project-SQL-Data-Warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------'

		-- ERP Product Category Data
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\User\Desktop\Project-SQL-Data-Warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------'
		SET @batch_end_time = GETDATE();

		/* ----------------------------------------------------------------------
           SECTION 3: Completion Summary
           ----------------------------------------------------------------------
           Provides execution summary for operational monitoring.
        ---------------------------------------------------------------------- */

		PRINT '=============================================================================';
		PRINT 'Loading Bronze Layer Is Completad'
		PRINT '    - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=============================================================================';

	END TRY
	BEGIN CATCH
		PRINT '=============================================================================';
		PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOAD';
		PRINT 'Message: ' + ERROR_MESSAGE();
		PRINT 'Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT 'Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT '=============================================================================';
	END CATCH
END
