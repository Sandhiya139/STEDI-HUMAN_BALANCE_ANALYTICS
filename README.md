STEDI HUMAN BALANCE ANALYTICS

Project Overview

This project demonstrates how a data lakehouse can be built using AWS services to process customer and sensor data collected from the STEDI Step Trainer System. The main objective is to prepare clean and trusted data that can be used for machine learning while protecting customer privacy.

Technologies Used

1. Amazon S3
2. AWS Glue
3. AWS Glue Studio
4. AWS Data Catalog
5. Amazon Athena
6. Apache Spark (PySpark)
7. Python
8. SQL
9. Github

Project Workflow

1. Upload the raw JSON datasets to Amazon S3.
2. Create Glue tables for the landing data.
3. Verify the data using Amazon Athena.
4. Create AWS Glue ETL jobs to process the data.
5. Build the truested and curated datasets.
6. Generate the final machine learning dataset.
7. Store SQL scripts, python scripts, and screenshots in this repository.

Project Stages

Landing Zone - Stores the original raw JSON datasets.
Trusted Zone - Contains only customers who agreed to share their information for research.
Curated Zone - Contains cleaned and processed datasets prepared for machine learning.

Project Outcome

By completing this project, I gained practical experience in building a cloud-based data pipeline using AWS services. I learned howto organize data into different zones, create ETL jobs with AWS Glue, query data using Athena, and prepare datasets for machine learning.
