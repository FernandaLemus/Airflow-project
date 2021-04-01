# Airflow-project

## This is a repository of a pipeline in Airflow.

### The pipeline is designed for the following purposes:
    - Reading files from s3 bucket (stage_redshift operator)
    - Take the s3 files to a staging tables in Redshift through coppy command instruction (stage_redshift operator)
    - Creating dimension and fact tables in Redshift from staging tables (load_dimension, load_fact operators)
    - Making simple data validations (data_quality operator)
