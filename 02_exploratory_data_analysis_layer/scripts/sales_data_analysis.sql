/* =============================================================================
   Exploratory Data Analysis (EDA)
   =============================================================================
   Script Purpose:
       This project performs an in-depth exploratory analysis of the Gold Layer 
       in a data warehouse modeled as a Star Schema. 
       The goal is to better understand data structure, quality, and business trends.

   The analysis includes:
       - Object exploration (tables, columns, data model validation)
       - Dimension exploration (customers, products, countries)
       - Measure exploration (sales, prices, quantities)
       - Magnitude analysis (aggregations by category, country, gender)
       - Ranking analysis (top and bottom customers, products, subcategories)
       - Data integrity checks (duplicates, missing values, unique keys)

   Business Purpose:
       To uncover actionable insights about customer behavior, sales performance, 
       and product contribution. 
       This analysis supports data-driven decision making and model validation 
       for downstream analytics and BI dashboards.

   Tools & Environment:
       - SQL (T-SQL)
       - Database: Gold Layer (Data Warehouse)
       - Schema Model: Star Schema (fact_sales, dim_customers, dim_products)
   ============================================================================= */


-- ===================================================================
-- OBJECTS EXPLORATION
-- ===================================================================
-- Explore all database objects to understand the structure of the schema
SELECT *
FROM INFORMATION_SCHEMA.TABLES
ORDER BY TABLE_SCHEMA;

-- Explore all columns in a specific table to get familiar with its attributes
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers'; -- Get a sense of the table’s structure
-- Note: Useful for identifying column types, keys, and potential NULLs before analysis

-- ===================================================================
-- DIMENSIONS EXPLORATION
-- ===================================================================
-- Explore distinct customer countries to understand geographic distribution
SELECT DISTINCT
	country
FROM gold.dim_customers;

-- Explore product categories, subcategories, and names to understand hierarchy and diversity
SELECT DISTINCT
	category,
	subcategory_id,
	product_name
FROM gold.dim_products
ORDER BY 1, 2, 3;
-- Note: Helps validate dimension completeness and check for missing or NULL categories

-- Analyze the range of order dates to understand data time coverage
SELECT
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	DATEDIFF(year, MIN(order_date), MAX(order_date)) AS order_range_years,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS order_range_months
FROM gold.fact_sales;
-- Note: Important for identifying the period over which the data spans and seasonality potential

-- Analyze customer age distribution and overall age range
SELECT
	MIN(birthdate) AS oldest_birthdate,
	DATEDIFF(year, MIN(birthdate), GETDATE()) AS oldest_age,
	MAX(birthdate) AS youngest_birthdate,
	DATEDIFF(year, MAX(birthdate), GETDATE()) AS youngest_age,
	DATEDIFF(year, MIN(birthdate), MAX(birthdate)) AS birthdate_range_years
FROM gold.dim_customers;
-- Note: Can reveal data entry errors and help segment customers by age

-- ===================================================================
-- MEASURES EXPLORATION
-- ===================================================================
-- Calculate total sales value across all orders
SELECT 
	SUM(sales_amount) AS total_sales
FROM gold.fact_sales;

-- Calculate total quantity of items sold
SELECT 
	SUM(quantity) AS total_quantity
FROM gold.fact_sales;

-- Calculate average product price across all sales
SELECT 
	AVG(price) AS avg_price
FROM gold.fact_sales;

-- Compare total order count to distinct order count to detect possible duplicates
SELECT 
	COUNT(order_number) AS total_orders_count,
	COUNT (DISTINCT order_number) AS total_distinct_orders_count -- Large differences may indicate duplicated data.
FROM gold.fact_sales;
-- Note: Essential for data quality checks before aggregation analysis

-- Identify possible duplicate orders or multiple items per order for further investigation
SELECT 
	order_number,
	COUNT(order_number) AS items_per_order
FROM gold.fact_sales
GROUP BY order_number;
-- Note: Useful for confirming that multiple items per order exist and are expected

-- Final inspection for a specific order number showing multiple items under one order
SELECT *
FROM gold.fact_sales
WHERE order_number = 'SO55367'
-- Note: Example to manually inspect detailed order data

-- Count total and distinct products to confirm product_key uniqueness
SELECT
	COUNT(product_key) AS total_products,
	COUNT(DISTINCT product_key) AS total_products_distinct -- All product keys are unique.
FROM gold.dim_products;

-- Count total and distinct customers to confirm customer_key uniqueness
SELECT
	COUNT(customer_key) AS total_customers,
	COUNT(DISTINCT customer_key) AS total_customers_distinct -- All customer keys are unique.
FROM gold.dim_customers;

-- Count total and distinct customers who placed an order
SELECT
	COUNT(customer_key) AS total_customers,
	COUNT(DISTINCT customer_key) AS total_customers_distinct 
FROM gold.fact_sales;
-- Note: Helps understand how many customers are active vs inactive

-- Generate a summary report that combines all main business KPIs (Key Performance Indicator)
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', COUNT(product_name) FROM gold.dim_products    -- Unique values.
UNION ALL
SELECT 'Total Nr. Customers', COUNT(customer_key) FROM gold.dim_customers; -- Unique values.
-- Note: This summary is useful for executive dashboards and initial EDA reports

-- ===================================================================
-- Magnitude Analysis
-- ===================================================================
-- Analyze total number of customers per country
SELECT
	country,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Analyze total number of customers, sales and average sales amount by gender
SELECT
    dc.gender,
    COUNT(DISTINCT dc.customer_key) AS num_customers,
    AVG(fs.sales_amount) AS avg_sales,
    SUM(fs.sales_amount) AS total_sales
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
ON        fs.customer_key = dc.customer_key
GROUP BY dc.gender
ORDER BY avg_sales DESC;
-- Note: Identifies gender-related trends and potential segments for marketing

-- Analyze total number of products by category
SELECT
	category,
	COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;

-- Analyze average product cost per category
SELECT
	category,
	AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC;
-- Note: Highlights cost patterns and potential margin insights

-- Analyze total revenue by product category
SELECT 
	dp.category,
	SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
ON        fs.product_key = dp.product_key
GROUP BY dp.category
ORDER BY total_revenue DESC;
-- Note: Reveals high-revenue categories for prioritization

-- Analyze total revenue per customer
SELECT
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
ON        fs.customer_key = dc.customer_key
GROUP BY
	dc.customer_key,
	dc.first_name,
	dc.last_name
ORDER BY total_revenue DESC;
-- Note: Helps identify top customers and high-value clients

-- Analyze total items sold by country
SELECT
	dc.country,
	SUM(fs.quantity) AS total_items_sold
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
ON        fs.customer_key = dc.customer_key
GROUP BY dc.country
ORDER BY total_items_sold DESC;
-- Note: Useful for geographic demand analysis

-- ===================================================================
-- Ranking Analysis
-- ===================================================================
-- Identify top 5 best-selling products by revenue
SELECT TOP 5
	dp.product_name,
	SUM(fs.sales_amount) AS total_product_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
ON        fs.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY total_product_revenue DESC;

-- Alternative method for ranking top 5 products using ROW_NUMBER()
SELECT *
FROM
	(SELECT
		ROW_NUMBER() OVER(ORDER BY SUM(fs.sales_amount) DESC) AS products_rank,
		dp.product_name,
		SUM(fs.sales_amount) AS total_product_revenue
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_products dp
	ON        fs.product_key = dp.product_key
	GROUP BY dp.product_name ) t
WHERE products_rank <= 5
-- Note: Shows alternative approach using window functions

-- Identify bottom 5 least-selling products by revenue
SELECT TOP 5
	dp.product_name,
	SUM(fs.sales_amount) AS total_product_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
ON        fs.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY total_product_revenue ASC;

-- Identify top 5 subcategories by total revenue
SELECT TOP 5
	dp.subcategory_id,
	SUM(fs.sales_amount) AS total_subcategory_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
ON        fs.product_key = dp.product_key
GROUP BY dp.subcategory_id
ORDER BY total_subcategory_revenue DESC;

-- Identify bottom 5 subcategories by total revenue
SELECT TOP 5
	dp.subcategory_id,
	SUM(fs.sales_amount) AS total_subcategory_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
ON        fs.product_key = dp.product_key
GROUP BY dp.subcategory_id
ORDER BY total_subcategory_revenue ASC;

-- Find customers with one or fewer orders (low-engagement customers)
SELECT
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	COUNT(DISTINCT fs.order_number) AS num_of_orders
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
ON        fs.customer_key = dc.customer_key
GROUP BY
	dc.customer_key,
	dc.first_name,
	dc.last_name
HAVING COUNT(DISTINCT fs.order_number) <= 1;
-- Note: Useful for retention strategy and identifying low-engagement customers

-- Find highly active customers with more than 26 orders
SELECT
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	COUNT(DISTINCT fs.order_number) AS num_of_orders
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
ON        fs.customer_key = dc.customer_key
GROUP BY
	dc.customer_key,
	dc.first_name,
	dc.last_name
HAVING COUNT(DISTINCT fs.order_number) > 26;
-- Note: Identifies high-value, loyal customers for potential rewards or VIP programs

-- Percentage of customers by number of orders
WITH customer_orders AS (
    SELECT
        dc.customer_key,
        COUNT(DISTINCT fs.order_number) AS num_of_orders
    FROM gold.fact_sales fs
    LEFT JOIN gold.dim_customers dc
    ON        fs.customer_key = dc.customer_key
    GROUP BY dc.customer_key
),
order_distribution AS (
    SELECT
