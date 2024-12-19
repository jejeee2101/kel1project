from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, col, regexp_replace
from pyspark.sql import functions as F
import re
from airflow.models import Variable
from cryptography.fernet import Fernet

# Default args for DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['data_team@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# DAG definition
dag = DAG(
    'final_project',
    default_args=default_args,
    description='Analisis distribusi penyebab kematian berdasarkan demografi',
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
)

# Fetch the encryption key from Airflow's Variable
ENCRYPTION_KEY = Variable.get("ENCRYPTION_KEY").encode()  # Fetch key and encode it
fernet = Fernet(ENCRYPTION_KEY)  # Use the key to create a Fernet object

# Function to start SparkSession
def get_spark_session():
    return SparkSession.builder \
        .appName("Death Cause Analysis") \
        .config("spark.jars", "/home/hadoop/postgresql-42.2.26.jar") \
        .getOrCreate()

# Function to encrypt names
def encrypt_name(data):
    data["name"] = data["name"].apply(
        lambda x: fernet.encrypt(x.encode()).decode() if isinstance(x, str) else x
    )
    return data

# Function to mask and encrypt data
def mask_and_encrypt_data(**kwargs):
    spark = get_spark_session()

    jdbc_url = "jdbc:postgresql://172.17.58.164:5432/final_project"
    jdbc_properties = {
        "user": "postgres",
        "password": "afaz2293",
        "driver": "org.postgresql.Driver"
    }

    input_table = "bencana_data"
    data = spark.read.jdbc(url=jdbc_url, table=input_table, properties=jdbc_properties)

    # Mask numbers after 'No.' in 'address' column
    data = data.withColumn(
        "address",
        regexp_replace(col("address"), r"(No\.\s)(\d+)", r"\1**")
    )

    # Convert the DataFrame to pandas for further operations (encryption)
    pandas_data = data.toPandas()

    # Apply name encryption
    pandas_data = encrypt_name(pandas_data)

    # Convert back to Spark DataFrame
    data = spark.createDataFrame(pandas_data)

    # Sort by 'id'
    data = data.orderBy("id")

    # Save the masked and encrypted data to PostgreSQL
    data.write.jdbc(
        url=jdbc_url,
        table="masked_and_encrypted_bencana_data",
        mode="overwrite",
        properties=jdbc_properties
    )

    # Save the masked and encrypted data to CSV
    data.write.option("delimiter", ";").mode("overwrite").csv("/home/hadoop/data/masked_and_encrypted_bencana_data.csv", header=True)
    spark.stop()

# Task for analyzing demographic distribution of death causes
def analyze_demographic_distribution(**kwargs):
    spark = get_spark_session()

    jdbc_url = "jdbc:postgresql://172.17.58.164:5432/final_project"
    jdbc_properties = {
        "user": "postgres",
        "password": "afaz2293",
        "driver": "org.postgresql.Driver"
    }

    input_table = "bencana_data"
    data = spark.read.jdbc(url=jdbc_url, table=input_table, properties=jdbc_properties)

    if 'age_group' not in data.columns:
        data = data.withColumn(
            "age_group",
            when(col("age") < 13, "child")
            .when((col("age") >= 13) & (col("age") < 20), "teenager")
            .when((col("age") >= 20) & (col("age") < 60), "adult")
            .otherwise("elderly")
        )

    demographic_analysis = data.groupBy("age_group", "gender", "cause").agg(count("*").alias("death_count"))

    # Sort by age_group order
    demographic_analysis = demographic_analysis.orderBy(
        F.when(col("age_group") == "child", 1)
        .when(col("age_group") == "teenager", 2)
        .when(col("age_group") == "adult", 3)
        .when(col("age_group") == "elderly", 4)
    )

    # Save to PostgreSQL
    demographic_analysis.write.jdbc(
        url=jdbc_url,
        table="demographic_distribution_analysis",
        mode="append",
        properties=jdbc_properties
    )

    # Save output to CSV
    demographic_analysis.write.option("delimiter", ";").mode("overwrite").csv("/home/hadoop/data/demographic_distribution_analysis.csv", header=True)

    spark.stop()

# Task for analyzing death count per province
def analyze_death_per_province(**kwargs):
    spark = get_spark_session()

    jdbc_url = "jdbc:postgresql://172.17.58.164:5432/final_project"
    jdbc_properties = {
        "user": "postgres",
        "password": "afaz2293",
        "driver": "org.postgresql.Driver"
    }

    input_table = "bencana_data"
    data = spark.read.jdbc(url=jdbc_url, table=input_table, properties=jdbc_properties)

    death_per_province = data.groupBy("province").agg(count("*").alias("death_count"))

    # Sort by death_count descending
    death_per_province = death_per_province.orderBy(col("death_count").desc())

    # Save to PostgreSQL
    death_per_province.write.jdbc(
        url=jdbc_url,
        table="death_per_province_analysis",
        mode="append",
        properties=jdbc_properties
    )

    # Save output to CSV
    death_per_province.write.option("delimiter", ";").mode("overwrite").csv("/home/hadoop/data/death_per_province_analysis.csv", header=True)

    spark.stop()

# Task for analyzing the output files
def analyze_output_files(**kwargs):
    import os
    output_files = [
        "/home/hadoop/data/demographic_distribution_analysis.csv",
        "/home/hadoop/data/death_per_province_analysis.csv",
        "/home/hadoop/data/masked_and_encrypted_bencana_data.csv"
    ]
    
    for file in output_files:
        if os.path.exists(file):
            print(f"File {file} exists.")
        else:
            print(f"File {file} does not exist.")

# Task for analyzing the output files
analyze_output_files_task = PythonOperator(
    task_id='analyze_output_files',
    python_callable=analyze_output_files,
    dag=dag,
)

# Task for analyzing demographic distribution of death causes
analyze_demographic_task = PythonOperator(
    task_id='analyze_demographic_distribution',
    python_callable=analyze_demographic_distribution,
    dag=dag,
)

# Task for analyzing death count per province
analyze_death_per_province_task = PythonOperator(
    task_id='analyze_death_per_province',
    python_callable=analyze_death_per_province,
    dag=dag,
)

# Task for masking and encrypting data
mask_and_encrypt_task = PythonOperator(
    task_id='mask_and_encrypt_data',
    python_callable=mask_and_encrypt_data,
    dag=dag,
)

# Task execution order
analyze_demographic_task >> analyze_death_per_province_task >> mask_and_encrypt_task >> analyze_output_files_task