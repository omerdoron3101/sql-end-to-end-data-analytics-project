# End-to-End SQL Data Analytics Project

This project unifies three SQL-based analytics projects - **Data Warehouse & Analytics**, **Exploratory Data Analysis (EDA)**, and **Advanced Retail Sales Analytics** - into one comprehensive end-to-end solution.  
It demonstrates the complete data lifecycle, from **data warehousing and modeling** to **exploratory analysis and business insights**.  
The project follows a layered architecture and highlights real-world data engineering and analytical practices.

---

## 🧱 Project Architecture
The solution follows a layered approach based on the Medallion Architecture:

1. **Data Warehouse Layer (Bronze → Silver → Gold)**  
   - Bronze: Raw data ingestion from CSV files  
   - Silver: Cleansing, standardization, and normalization  
   - Gold: Business-ready data modeled into a Star Schema (fact and dimension tables)

2. **Exploratory Data Analysis (EDA) Layer**  
   - SQL-based analysis on fact and dimension tables  
   - Aggregations, CTEs, JOINs, ranking, and segmentation to explore sales, products, and customers  

3. **Advanced Data Analytics: Retail Sales Analytics Layer**  
   - Advanced queries to calculate KPIs (AOV, monthly revenue, moving averages)  
   - Actionable insights for inventory management, marketing strategy, and product optimization

---

## 1. Data Warehouse and Analytics Project

This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.
<img width="2207" height="1071" alt="_Data Architecture drawio" src="https://github.com/user-attachments/assets/65ba574f-121e-4869-af4f-428521dc1a08" />

### 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:


1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

### 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

---

## 2. Exploratory Data Analysis (EDA) Project
 
This project explores and analyzes a retail sales dataset stored in a star schema model using **SQL**.
The goal is to uncover business insights about customers, products and sales performance.


### 🧠 Tools & Technologies

- SQL (T-SQL / Microsoft SQL Server)
- Star Schema Data Model (fact_sales, dim_customers, dim_products)
- Gold Layer: Data Warehouse environment


### 📊 Key Topics Covered

- Data exploration and validation
- Dimension & fact table analysis
- Aggregations and KPIs
- Ranking and segmentation of products and customers

---

## 3. Advanced Data Analytics Project: Retail Sales Analytics

This project demonstrates **comprehensive SQL-based analysis** on a retail sales dataset.  
The goal is to extract **actionable business insights** from historical sales data, customer behaviors, and product performance.

The analysis leverages a **data warehouse approach**, including aggregated measures at the **product**, **customer**, and **category** levels.  
It provides insights for decision-making in areas such as inventory management, marketing strategy, and sales optimization.

### Objectives
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

### Data Sources
The project uses a **star schema** design in a data warehouse (`Gold Layer`):

- **Fact Table**:
  - `fact_sales`: contains transactional sales data including order numbers, customer keys, product keys, sales amount, quantity, price, and order date.  
- **Dimension Tables**:
  - `dim_products`: contains product information such as name, category, subcategory, and cost.  
  - `dim_customers`: contains customer information including first name, last name, birthdate, and customer number.  


### Technologies & Tools
- **SQL Server / T-SQL:** For data extraction, transformation, and aggregation  
- **Data Warehouse Concepts:** Star schema design (fact and dimension tables)  
- **Window Functions & Aggregations:** Running totals, moving averages, LAG/LEAD, partitioning  
- **Business KPIs:** AOV, monthly revenue, recency, lifespan  

---

## 🌟 About Me

👋 Hi! I'm Omer Doron
I’m a student of Information Systems specializing in Digital Innovation.
I’m passionate about transforming raw information into meaningful insights.

I created this project as part of my learning journey in data warehousing and analytics, and as a showcase of my technical and analytical skills.

🔗 [Connect with me on LinkedIn](https://www.linkedin.com/in/omer-doron-a070732b1/)
