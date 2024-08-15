# Pipeline for Detecting and Correcting 1NF Anomalies

## Project Overview

This project is designed to detect anomalies in a relational database that violate the First Normal Form (1NF) and automatically correct them to optimize the performance of the database management system (DBMS). The project leverages a pipeline that detects, analyzes, and normalizes database tables to ensure they adhere to 1NF standards.

## Features

- **Anomaly Detection**: Automatically identifies anomalies in database tables, such as non-atomic values or repeating groups.
- **Data Normalization**: Corrects detected anomalies by restructuring the tables to comply with 1NF.
- **Performance Optimization**: Enhances the efficiency and performance of the DBMS by ensuring the database adheres to normalization rules.
- **Automation**: The entire process is automated using Apache Airflow, enabling scheduled and continuous monitoring of the database.

## Tools and Technologies

- **Apache Airflow**: Used to orchestrate and automate the anomaly detection and correction pipeline.
- **Python**: The core programming language used to implement the anomaly detection logic.
- **SQLAlchemy**: A SQL toolkit and Object-Relational Mapping (ORM) library for Python, used to interact with the PostgreSQL database.
- **PostgreSQL**: The relational database management system (RDBMS) used in this project.

## Project Structure

```plaintext
detection_anomalies/
│
├── dags/
│   ├── detect_1FN_anomaly-and_normalise.py     # DAG for detecting and correcting 1NF anomalies
│   ├── __pycache__/
│       ├── detect_1FN_anomaly.cpython-39.pyc   # Compiled Python files
│       ├── detect_anomaly_dag.cpython-39.pyc
│
├── docker-compose.yml                          # Docker Compose file for setting up the environment
│
└── README.md                                   # Project documentation


