/* =============================================================================
   DDL Script: Create Gold Layer Views
   ============================================================================= 

   Script Purpose:
       This script creates the views that define the Gold Layer of the 
       Data Warehouse. These views are designed to combine, enrich, and 
       standardize data from the Silver Layer, transforming it into a 
       business-ready model for analytics and reporting.

   ⚠️ Warning:
       Running this script will DROP and RECREATE all existing Gold Layer views.
       Any dependent reports, dashboards, or queries might be temporarily 
       unavailable during the recreation process.

   ============================================================================= */
  
/* -----------------------------------------------------------------------------
   View: gold.dim_customers
   Purpose:
       Creates the Customer Dimension view that consolidates CRM and ERP 
       customer data into a unified customer profile.

   Transformation Logic:
       - Joins CRM customer info with ERP customer attributes and location data.
       - Generates a surrogate key using ROW_NUMBER().
       - Normalizes gender using both CRM and ERP sources.
       - Ensures one record per customer with standardized fields.
   ----------------------------------------------------------------------------- */
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
	SELECT
		ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,          -- Surrogate key
		ci.cst_id AS customer_id,                                       -- Natural key from CRM
		ci.cst_key AS customer_number,                                  
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE 
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
			ELSE COALESCE(ca.gen, 'n/a')
		END AS gender,                                                  -- Gender resolved from both sources
		ca.bdate AS birthdate,
		ci.cst_create_date AS create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON        ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON        ci.cst_key = la.cid;
GO


/* -----------------------------------------------------------------------------
   View: gold.dim_products
   Purpose:
       Creates the Product Dimension view that merges CRM product details 
       with ERP product category data. Provides a complete business view of 
       each product.

   Transformation Logic:
       - Joins product metadata with ERP category mapping.
       - Generates a surrogate product key.
       - Filters out historical product records.
   ----------------------------------------------------------------------------- */
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
	SELECT
		ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,  -- Surrogate key
		pn.prd_id AS product_id,
		pn.prd_key AS product_number,
		pn.prd_nm AS product_name,
		pn.cat_id AS category_id,
		pc.cat AS category,
		pc.cubcat AS subcategory_id,
		pc.maintenance,
		pn.prd_cost AS cost,
		pn.prd_line AS product_line,
		pn.prd_start_dt AS start_date
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON        pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL;                                         -- Keep only active products
GO


/* -----------------------------------------------------------------------------
   View: gold.fact_sales
   Purpose:
       Creates the Sales Fact view, linking transactional sales data to 
       customers and products.

   Transformation Logic:
       - Links each sale to its corresponding customer and product.
       - Retains only relevant business columns for analytical use.
   ----------------------------------------------------------------------------- */
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
	SELECT
		sd.sls_ord_num AS order_number,
		dp.product_key,
		dc.customer_key,
		sd.sls_order_dt AS order_date,
		sd.sls_ship_dt AS shipping_date,
		sd.sls_due_dt AS due_date,
		sd.sls_sales AS sales_amount,
		sd.sls_quantity AS quantity,
		sd.sls_price AS price
	FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_products dp
	ON		  sd.sls_prd_key = dp.product_number
	LEFT JOIN gold.dim_customers dc
	ON		  sd.sls_cust_id = dc.customer_id;
GO
