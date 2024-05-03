Sparkify startup requested to automate and monitor their data warehouse ETL pipelines.

Using Apache Airflow, Extract, Transform and Load (ETL) pipeline extracts JSON data files from Amazon Simple Storage Service (S3) data storage, processes and loads them into a star schema relational database on Amazon Redshift data warehouse.

Data quality tests run after the ETL steps have been executed to catch any discrepancies in the datasets.

![image](https://github.com/vijayaraghavanka/data-pipelines-airflow-sparkify/assets/165424511/e0dc0be4-e346-44ed-b73b-f54b7a038809)

Data for song and user activities reside in S3 as JSON files:

Song data: s3://udacity-dend/song_data copied to a bucket owned by me
Log data: s3://udacity-dend/log_data copied to a bucket owned by me
Log data json path: s3://udacity-dend/log_json_path.json copied to a bucket owned by me


Project files
├── create_tables.sql
├── dags
│   ├── final_project.py
└── plugins
    ├── operators
    │   ├── create_table.py
    │   ├── stage_redshift.py
    │   ├── load_fact.py
    │   ├── load_dimension.py
    │   └── data_quality.py   
    └── helpers
        └── sql_queries.py    
