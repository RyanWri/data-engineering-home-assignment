# Data Engineering Assignment Documentation

## **Overview**
This document provides a summary of the steps taken to complete the data engineering assignment, including the use of **Pandas**, **PySpark**, **AWS Glue**, **CloudFormation**, and **Athena**. It also highlights potential improvements and limitations due to time constraints.

---

## **Steps Taken**

### **1. Exploring the Data with Pandas**
- Initially, we used **Pandas** to load and analyze the provided `stocks_data.csv`.
- Key goals:
  - Understand the dataset structure.
  - Handle missing values.
  - Calculate metrics like daily returns, volatility, and worth.
- Results helped validate the logic for calculations in a small-scale environment.

---

### **2. Scaling with PySpark (Locally)**
- Migrated the logic to **PySpark** for distributed data processing.
- Implemented the following calculations:
  - **Average Daily Return** (for all stocks per date).
  - **Highest Worth Stock** (by average daily worth: closing price ÷ volume).
  - **Most Volatile Stock** (using annualized standard deviation of daily returns).
  - **Top 3 30-Day Returns** (highest percent increase in 30-day closing price).
- Ran locally with PySpark to validate distributed processing logic.

---

### **3. Converting to an AWS Glue Job**
- Adapted the PySpark code for **AWS Glue**:
  - Integrated AWS Glue utilities like `getResolvedOptions`.
  - Configured the Glue job to accept parameters for input and output S3 paths.
  - Output results as CSV files in S3 for each question.

---

### **4. Creating a Glue Catalog Database with CloudFormation**
- Defined a **Glue Catalog Database** in a **CloudFormation** template to store metadata for the results.
- Used CloudFormation to manage infrastructure as code, ensuring repeatability and consistency.

---

### **5. Creating an IAM Role**
- Configured an **IAM Role** for Glue:
  - Allowed access to the S3 bucket containing results.
  - Included permissions for creating tables and interacting with AWS Glue services.

---

### **6. Creating Matching Crawlers**
- Created **Glue Crawlers** to populate tables in the Glue Catalog for each result:
  - Crawlers matched S3 folders for `question_1`, `question_2`, etc.
  - Each crawler was named uniquely for identification.

---

### **7. Automating Triggers**
- Developed scripts using **Boto3** to:
  - Trigger the Glue job with parameters for input and output paths.
  - Start the crawlers after the Glue job finished processing.
  - Monitored the status of crawlers and ensured successful execution.

---

### **8. Testing in Athena Console**
- Queried the resulting Glue Catalog tables in **Athena**:
  - Validated schema integrity and data correctness.
  - Verified outputs for all questions.

---

## **Improvements and Next Steps**

### **1. Better Partitioning**
- Partitioning the Glue Catalog tables (e.g., by `Date` or `ticker`) could significantly improve query performance in Athena.

### **2. Integrate CloudFormation into CI/CD**
- Automating CloudFormation stack updates via a CI/CD pipeline would ensure seamless deployment and consistency across environments.

### **3. Add Testing**
- Implement **unit tests** for Pandas and PySpark transformations.
- Develop **integration tests** to validate Glue job outputs and crawler results.

---

## **Time Constraints**
This project was completed under limited time, focusing on core functionality and delivering a working solution. While some optimizations and refinements are possible, the implementation is robust and extensible. 

If you have any questions or need further clarification, feel free to reach out.
