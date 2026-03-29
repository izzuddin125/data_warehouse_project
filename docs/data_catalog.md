# Data Dictionary For Gold Layer

## Overview
<br>The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. it consist of dimension tables and fact tables for specific business metrics.<br>
### 1. gold.dim_customer
- **Purpose**: Stores customer details enriched with demographic and geographic data
- **Columns**:

| Column Name | Data Type | Description |
| :----------- |:--------------| :-------------|
| customer_key| BIGINT | Surrogate key uniquely identifying each customer record in the dimension table |
| customer_id | INT | Unique numerical identifier assigned to each customer |
| customer_number | NVARCHAR(50) | Alphanumeric identifier representing the customer, used for tracking and referencing |
3. gold.dim_product
4. gold.fact_sales
- [ ] CHECKLIST
- LIST
<BR>
___BOLD TEXT___<BR>
**BOLD TEXT**

| Left-Aligned | Center-Aligned | Right-Aligned |
| :----------- |:--------------:| -------------:|
| This         | is             | an            |
| aligned      | text           | example       |
| example      | table          | right         |
