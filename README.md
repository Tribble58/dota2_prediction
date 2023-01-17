# Dota 2 Analytical System

## Discription

This project is aimed to create a unified system that contains Dota 2 data. Basically it will be designed as Data Warehouse for Machine Learning application.

## Main Workflow

Data is collected using Dota 2 official API (https://docs.opendota.com). \
During data modeling, the graphical data diagram was drawn:
<img height="1350" src="other\data_model.png" width="500"/>

The main integration tool is **Apache Airflow**, RDBMS is **PostgreSQL**. \
**PostgreSQL** is used both as intermediate storage for data that flows between DAGS and as the main storage for final data.