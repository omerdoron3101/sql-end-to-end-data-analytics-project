# Advanced Data Analytics Project: Retail Sales Analytics

This project demonstrates **comprehensive SQL-based analysis** on a retail sales dataset.  
The goal is to extract **actionable business insights** from historical sales data, customer behaviors, and product performance.

The analysis leverages a **data warehouse approach**, including aggregated measures at the **product**, **customer**, and **category** levels.  
It provides insights for decision-making in areas such as inventory management, marketing strategy, and sales optimization.

---

## Objectives
- Understand sales trends over time (yearly, monthly, and seasonally).  
- Identify **top-performing products and categories** and evaluate dependencies.  
- Segment customers based on **lifetime value, purchase frequency, and recency**.  
- Compute KPIs such as:
    - Average Order Value (AOV)  
    - Average Monthly Revenue/Spend  
    - Running totals and moving averages  
- Support strategic decisions for:
    - Inventory allocation  
    - Marketing campaigns  
    - Product portfolio optimization  

---

## Data Sources
The project uses a **star schema** design in a data warehouse (`Gold Layer`):

- **Fact Table**:
  - `fact_sales`: contains transactional sales data including order numbers, customer keys, product keys, sales amount, quantity, price, and order date.  
- **Dimension Tables**:
  - `dim_products`: contains product information such as name, category, subcategory, and cost.  
  - `dim_customers`: contains customer information including first name, last name, birthdate, and customer number.  

---

### ⚠️ Prerequisites

This project relies on the **views created in My "Data Warehouse and Analytics" project**.  
Before running the queries in `scripts`, ensure that the following views exist in your Gold Layer:

- `dim_customers`
- `dim_products`
- `fact_sales`

These views are required to replicate the results exactly as presented in the analysis.  
Please run the SQL scripts from the 🔗 [sql-data-warehouse-project](https://github.com/omerdoron3101/sql-data-warehouse-project/tree/main) to generate them.

---

## Analyses Performed

### 1. Product Report
**Purpose:** Consolidates product-level metrics to identify high, mid-range, and low performers.  
**Key Metrics:**
- Total orders, total customers, total sales, total quantity sold
- Product segments: High Performer, Mid Range Performer, Low Performer   
- Average selling price per product  
- Average order revenue and monthly revenue  
- Lifespan of product sales (in months)  
**Business Insight:** Identify top-performing products and evaluate revenue concentration by product.

---

### 2. Customer Report
**Purpose:** Provides a comprehensive view of customer behavior and segmentation.  
**Key Metrics:**
- Total orders, total spending, total quantity purchased  
- Total products purchased and lifespan of customer relationship  
- Customer segments: VIP, Regular, New  
- Age groups and recency (months since last order)  
- Average order value (AOV) and average monthly spend  
**Business Insight:** Helps target marketing campaigns, retention strategies, and loyalty programs.

---

### 3. Advanced Data Analytics
**Purpose:** Evaluates sales trends, cumulative performance, and part-to-whole contributions.  

**Sub-analyses:**

#### a) Changes Over Time
- Yearly, monthly, and combined month-year analysis  
- Identifies trends, seasonality, and anomalies  

#### b) Cumulative Analysis
- Running total sales and moving averages by month and year  
- Helps evaluate long-term business growth  

#### c) Performance Analysis
- Year-over-year comparison of product sales  
- Compares current sales against historical averages to detect underperformance or growth  

#### d) Part-To-Whole Analysis
- Analyzes category contributions to total sales and inventory  
- Evaluates over-dependence on specific categories (e.g., Bikes)  
- Assesses inventory versus profitability to optimize stock allocation  

#### e) Data Segmentation
- Segments products by cost ranges  
- Segments customers by spending behavior and engagement  
- Supports portfolio analysis and targeted marketing strategies  

---

## Technologies & Tools
- **SQL Server / T-SQL:** For data extraction, transformation, and aggregation  
- **Data Warehouse Concepts:** Star schema design (fact and dimension tables)  
- **Window Functions & Aggregations:** Running totals, moving averages, LAG/LEAD, partitioning  
- **Business KPIs:** AOV, monthly revenue, recency, lifespan  

---

## Key Insights
- **Product Concentration:** A small number of products (e.g., Bikes) generate the majority of revenue.  
- **Customer Value:** VIP customers contribute 36.66% to total sales.  
- **Seasonality:** Sales trends vary significantly by month, highlighting peak periods.  
- **Inventory vs Profitability:** Accessories constitute a large portion of inventory but contribute little to profit.  
- **Growth Opportunities:** Identify mid-range products and underperforming categories for strategic investment.  

