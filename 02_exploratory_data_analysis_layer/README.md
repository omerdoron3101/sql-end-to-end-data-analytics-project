# Exploratory Data Analysis (EDA) Project
 
This project explores and analyzes a retail sales dataset stored in a star schema model using **SQL**.
The goal is to uncover business insights about customers, products and sales performance.

---

### 🧠 Tools & Technologies

- SQL (T-SQL / Microsoft SQL Server)
- Star Schema Data Model (fact_sales, dim_customers, dim_products)
- Gold Layer: Data Warehouse environment

---

### 📊 Key Topics Covered

- Data exploration and validation
- Dimension & fact table analysis
- Aggregations and KPIs
- Ranking and segmentation of products and customers

---

### ⚠️ Prerequisites

This project relies on the **views created in My "Data Warehouse and Analytics" project**.  
Before running the queries in `scripts`, ensure that the following views exist in your Gold Layer:

- `dim_customers`
- `dim_products`
- `fact_sales`

These views are required to replicate the results exactly as presented in the analysis.  
Please run the SQL scripts from the 🔗 [sql-data-warehouse-project](https://github.com/omerdoron3101/sql-data-warehouse-project/tree/main) to generate them.
