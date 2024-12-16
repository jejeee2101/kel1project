from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, col, regexp_replace
from airflow.models import Variable
from cryptography.fernet import Fernet

# Default args for DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['data_team@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'bismillah',
    default_args=default_args,
    description='Analisis distribusi penyebab kematian berdasarkan demografi',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 1),
    catchup=False,
)

# Fetch the encryption key from Airflow's Variable
ENCRYPTION_KEY = Variable.get("ENCRYPTION_KEY").encode()
fernet = Fernet(ENCRYPTION_KEY)

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

# Task: Mask and Encrypt Data
def mask_and_encrypt_data(**kwargs):
    spark = get_spark_session()

    jdbc_url = "jdbc:postgresql://127.0.0.1:5432/cika"
    jdbc_properties = {
        "user": "postgres",
        "password": "cika",
        "driver": "org.postgresql.Driver"
    }

    input_table = "bencana_data"
    data = spark.read.jdbc(url=jdbc_url, table=input_table, properties=jdbc_properties)

    # Mask numbers in 'address'
    data = data.withColumn(
        "address",
        regexp_replace(col("address"), r"(No\.\s)(\d+)", r"\1**")
    )

    # Encrypt 'name'
    pandas_data = data.toPandas()
    pandas_data = encrypt_name(pandas_data)

    # Convert back to Spark DataFrame and sort by id
    data = spark.createDataFrame(pandas_data).orderBy("id")

    # Save to PostgreSQL
    data.write.jdbc(
        url=jdbc_url,
        table="masked_and_encrypted_bencana_data",
        mode="overwrite",
        properties=jdbc_properties
    )

    # Save to CSV
    data.write.option("delimiter", ";").csv("/home/hadoop/data/masked_and_encrypted_bencana_data.csv", header=True)

    spark.stop()

# Task: Analyze Demographic Distribution
def analyze_demographic_distribution(**kwargs):
    spark = get_spark_session()

    jdbc_url = "jdbc:postgresql://127.0.0.1:5432/cika"
    jdbc_properties = {
        "user": "postgres",
        "password": "cika",
        "driver": "org.postgresql.Driver"
    }

    input_table = "bencana_data"
    data = spark.read.jdbc(url=jdbc_url, table=input_table, properties=jdbc_properties)

    # Add age_group column
    data = data.withColumn(
        "age_group",
        when(col("age") < 13, "child")
        .when((col("age") >= 13) & (col("age") < 20), "teenager")
        .when((col("age") >= 20) & (col("age") < 60), "adult")
        .otherwise("senior")
    )

    # Perform analysis
    demographic_analysis = data.groupBy("age_group", "gender", "cause") \
        .agg(count("*").alias("death_count")) \
        .orderBy("age_group")  # Order by age_group: child -> senior

    # Save to PostgreSQL
    demographic_analysis.write.jdbc(
        url=jdbc_url,
        table="demographic_distribution_analysis",
        mode="append",
        properties=jdbc_properties
    )

    # Save to CSV
    demographic_analysis.write.option("delimiter", ";").csv("/home/hadoop/data/demographic_distribution_analysis.csv", header=True)

    spark.stop()

# Task: Analyze Death Per Province
def analyze_death_per_province(**kwargs):
    spark = get_spark_session()

    jdbc_url = "jdbc:postgresql://127.0.0.1:5432/cika"
    jdbc_properties = {
        "user": "postgres",
        "password": "cika",
        "driver": "org.postgresql.Driver"
    }

    input_table = "bencana_data"
    data = spark.read.jdbc(url=jdbc_url, table=input_table, properties=jdbc_properties)

    # Perform analysis
    death_per_province = data.groupBy("province") \
        .agg(count("*").alias("death_count")) \
        .orderBy(col("death_count").desc())  # Order by death_count descending

    # Save to PostgreSQL
    death_per_province.write.jdbc(
        url=jdbc_url,
        table="death_per_province_analysis",
        mode="append",
        properties=jdbc_properties
    )

    # Save to CSV
    death_per_province.write.option("delimiter", ";").csv("/home/hadoop/data/death_per_province_analysis.csv", header=True)

    spark.stop()

# Define tasks in the DAG
mask_and_encrypt_task = PythonOperator(
    task_id='mask_and_encrypt_data',
    python_callable=mask_and_encrypt_data,
    dag=dag,
)

analyze_demographic_task = PythonOperator(
    task_id='analyze_demographic_distribution',
    python_callable=analyze_demographic_distribution,
    dag=dag,
)

analyze_death_per_province_task = PythonOperator(
    task_id='analyze_death_per_province',
    python_callable=analyze_death_per_province,
    dag=dag,
)

# Task execution order
analyze_demographic_task >> analyze_death_per_province_task >> mask_and_encrypt_task
