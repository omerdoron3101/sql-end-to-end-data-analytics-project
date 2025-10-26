/* =============================================================================
   Data Quality Checks: Silver Layer Tables
   ============================================================================= 

   Purpose:
       To detect and handle issues such as:
           - Duplicate records
           - Missing or invalid values
           - Inconsistent formatting (e.g., spaces, incorrect casing)
           - Logical inconsistencies (e.g., end date before start date)
   Usage:
       Run each script after data has been loaded into the Silver Layer.

   ============================================================================= */

/* =============================================================================
   CRM Tables
   ============================================================================= */

/* -----------------------------------------------------------------------------
   Data Quality Checks: silver.crm_cust_info
   ----------------------------------------------------------------------------- */

-- 1. Detect duplicate or missing customer IDs
SELECT
    cst_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2. Identify customer keys with unwanted leading/trailing spaces 
SELECT
    cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- 3. Identify first names with extra spaces 
SELECT
    cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- 4. Identify last names with extra spaces
SELECT
    cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- 5. Review all distinct marital status values for data consistency
SELECT DISTINCT
    cst_marital_status
FROM silver.crm_cust_info;

-- 6. Review all distinct gender values for valid categories
SELECT DISTINCT
    cst_gndr
FROM silver.crm_cust_info;

-- 7. Final data verification: inspect the entire cleaned dataset
SELECT *
FROM silver.crm_cust_info;


/* -----------------------------------------------------------------------------
   Data Quality Checks: silver.crm_prd_info
   ----------------------------------------------------------------------------- */

-- 1. Detect duplicate or missing product IDs
SELECT
    prd_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- 2. Identify product names with leading or trailing spaces 
SELECT
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- 3. Detect invalid or missing product costs (negative or NULL values) 
SELECT
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- 4. Review all distinct product line values for data consistency 
SELECT DISTINCT
    prd_line
FROM silver.crm_prd_info;

-- 5. Identify invalid date logic (end date earlier than start date)
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- 6. Final data verification: inspect the entire cleaned dataset
SELECT *
FROM silver.crm_prd_info;


/* -----------------------------------------------------------------------------
   Data Quality Checks: silver.crm_sales_details
   ----------------------------------------------------------------------------- */

-- 1. Check for NULL or unrealistic order dates 
SELECT
    sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_order_dt > '2050-01-01'
   OR sls_order_dt < '1900-01-01';

-- 2. Check for NULL or unrealistic ship dates
SELECT
    sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt IS NULL
   OR sls_ship_dt > '2050-01-01'
   OR sls_ship_dt < '1900-01-01';

-- 3. Check for NULL or unrealistic due dates
SELECT
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt IS NULL
   OR sls_due_dt > '2050-01-01'
   OR sls_due_dt < '1900-01-01';

-- 4. Identify logical inconsistencies between date fields
--    (e.g., order date cannot be later than ship or due date)
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- 5. Validate sales calculations and detect invalid numeric values
--    (Ensures sales = quantity * price)
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0;

-- 6. Final data verification: inspect the entire cleaned dataset
SELECT *
FROM silver.crm_sales_details;




/* =============================================================================
   ERP Tables
   ============================================================================= */

/* -----------------------------------------------------------------------------
   Data Quality Checks: silver.erp_cust_az12
   ----------------------------------------------------------------------------- */

-- 2. Validate birth dates for logical accuracy
SELECT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1900-01-01'
   OR bdate > GETDATE();

-- 3. Review distinct gender values for standardization
SELECT DISTINCT
	gen
FROM silver.erp_cust_az12;

-- 4. Final data verification: inspect the entire cleaned dataset
SELECT *
FROM silver.erp_cust_az12;


/* -----------------------------------------------------------------------------
   Data Quality Checks: silver.erp_loc_a101
   ----------------------------------------------------------------------------- */

-- 1. Review distinct cuntry values for standardization
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101;

-- 2. Final data verification: inspect the entire cleaned dataset
SELECT *
FROM silver.erp_loc_a101;


/* -----------------------------------------------------------------------------
   Data Quality Checks: silver.erp_px_cat_g1v2
   ----------------------------------------------------------------------------- */

SELECT
	cat,
	cubcat,
	maintenance
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(CAT)
   OR cubcat != TRIM(cubcat)
   OR maintenance != TRIM(maintenance);

SELECT DISTINCT
	cat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT
	cubcat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT
	maintenance
FROM silver.erp_px_cat_g1v2;

SELECT *
FROM silver.erp_px_cat_g1v2
