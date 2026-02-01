
🔍 Overview

This project delivers an end-to-end urban analytics lakehouse implemented on Google BigQuery, designed to identify structural investment gaps across metropolitan regions. The solution integrates demographic density, population distribution, public budget allocation, and green space availability into a unified analytical model, enabling data-driven prioritization of urban equity initiatives at scale.

🎯 Objective

To design and deploy a scalable data engineering and analytics pipeline that quantifies urban equity gaps and supports strategic decision-making for public investment optimization using standardized, production-grade data models.

🧠 Methodology

A medallion-style lakehouse architecture (Bronze, Silver, Gold) was implemented in BigQuery. Raw municipal datasets were ingested into the Bronze layer, cleansed and modeled into analytical dimensions and fact tables in the Silver layer, and exposed through KPI-ready views and clustering models in the Gold layer. A simulated large-scale monthly spending fact table was generated to replicate real-world public finance behavior, enabling advanced analytical use cases.

📊 Key Results

- Unified analytical model combining population, density, budget, and green space metrics at the municipal level  
- Equity Gap Score developed to rank regions by structural underinvestment risk  
- Monthly public spending fact table enabling trend and allocation analysis  
- Regional segmentation achieved through BigQuery ML clustering for strategic grouping  
- Analytics-ready outputs designed for executive dashboards and BI consumption  

🛠 Technologies & Skills

- Google BigQuery (Advanced SQL, Analytical Functions)  
- Lakehouse Architecture (Bronze / Silver / Gold)  
- Data Modeling (Dimensions, Fact Tables, Wide Tables)  
- BigQuery ML (K-Means Clustering)  
- KPI Design and Analytics Engineering  
- Data Quality Validation and Transformation Logic  

🧩 Why This Project Matters

Urban planning and public investment decisions require objective, scalable, and transparent analytics. This project demonstrates the ability to design production-ready data platforms that transform fragmented municipal data into actionable intelligence, enabling equitable resource allocation and long-term urban sustainability.
