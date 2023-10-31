# monitoring_case
Anomaly detection study.


## training_models notebook
This notebook  goes through the thought process around fitting and adapting the models. 
The models are trained using the first dataset data\transactions_1.csv.
After trained, the models are saved in pickle files in the models folder.

## transactions_functions.py
This python file creates the ModelProcessorOperator Class.

This class has the objective of simulating a processing pipeline for financial transactions:
It has the following functions:
- Receives financial transactions ocurrences,
- Processes the data.
- Apply the Predictive and the Rule-Based Model to check for anomalies.
- Uploads the Anomalies report to Bigquery or to a local csv file in output_files.
- Uploads the ocurrences transformed to Bigquery or to a local csv file in output_files.


## simulating_monitoring notebook
This file is used to simulate sending information from the test dataset to the database by importing and using the ModelProcessorOperator Class.

## data
- transactions_1.csv: financial transactions. Used to train the models.
- transactions_2.csv: financial transactions. Used to test the models.

## queries
-  running_anomalies: Queries the transactions_raw and anomalies_raw tables, and creates a view to check for ocurrences of anomalies in the last 10 minutes(running_count_10) for every model.
- create_tables: Creates the transactions_raw and anomalies_raw based on the schema.

