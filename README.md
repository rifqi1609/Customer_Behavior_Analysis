# Project Title
Customer Behavior Analysis  

## Repository Outline
1. README.md -  Overview of the project
3. postgre_ddl.txt - Query for generating table and import data to database
4. DAG_script.py - Notebook for implementing Airflow to extract the data from database (via postgre), clean the data, and push the data to elastic search.
5. DAG_graph.png - Image of DAG's process
6. raw_data.csv - Raw dataset
7. clean_data.csv - Clean dataset which is output from DAG
8. great_expectation.ipynb - Notebook for setting expectation to validate dataset
9. images - Folder fulfiled by data visualization
10. airflow_script.yaml - Script for composing container

## Problem Background
Knowing our customer is a must for deserving the best products and services. Their activities reflect how they spend they money and what they like. Undoubtedly, by understanding them well, we could gain much interactions to solve their problems and fulfill their need precisely.

## Project Output
This dashboard is purposed for marketing division to serve information about our customer. By that, we could give more targeted marketing to the most potential customer.

## Data
Dataset was collected from kaggle (https://www.kaggle.com/datasets/imakash3011/customer-personality-analysis).

## Method
This analysis implements Airflow for pipelining data process automatically. For data validation, Great Expectation was used for validate data. The last step was visualizing the analysis by Kibana (via Elastic Search)

## Stacks
1. Python
2. Airflow
3. Elastic Search
4. Kibana
5. Great Expectation
6. PostgreSQL