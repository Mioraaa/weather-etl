# Weather ETL Pipeline: From Visualcrossing API to Redshift via Airflow, S3, and Glue

A complete data pipeline to **Extract**, **Transform**, and **Load** (ETL) Weather data into an AWS Redshift data warehouse using modern cloud and orchestration tools.

## 🗺️ Table of Contents

* [🔍 Project Overview](#ProjectOverview)
* [⚙️ Stack & Technologies](#StackTechnologies)
* [🚀 Quick Start](#QuickStart)

##  1. <a name='ProjectOverview'></a>🔍 Project Overview

This pipeline illustrates the complete lifecycle of ingesting weather data from **VisualCrossing** and preparing it for advanced analysis and visualization in a **cloud-based data warehouse**.

---

##  2. <a name='StackTechnologies'></a>⚙️ Stack & Technologies

**Tools:** Docker, Airflow, AWS S3, AWS Glue, AWS Redshift
**Libraries/Tech:** `pyspark`, `s3fs`, `pytest`, `logging`

---

##  3. <a name='PipelineSteps'></a>🛠️ Pipeline Steps

1. **Scrape** data from Open weather using the `praw` API.
2. **Transform** the data with `pandas`.
3. **Push** the raw data to AWS S3 using `s3fs`.
4. **Automate** steps 1–3 using **Airflow DAGs**.
5. **Run Spark-based ETL jobs** using **AWS Glue**.
6. **Apply additional transformations** and move cleaned data to another S3 bucket.
7. **Load the final dataset** into **Amazon Redshift**, making it ready for BI tools like **Power BI** or **Tableau**.

---

##  4. <a name='QuickStart'></a>🚀 Quick Start

1. **Clone the repository:**

   ```bash
   git clone https://github.com/Mioraaa/weather-etl.git
   cd weather-etl
   ```

2. **Start Airflow and services:**

   ```bash
   docker compose up -d
   ```

6. **Access the Airflow UI**
   Open your browser and go to: [http://localhost:8081](http://localhost:8081)

7. **Trigger the DAG** to run the full ETL process.

---
