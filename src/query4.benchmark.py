from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from load import *
from query3 import *
from query4 import *
import time


# Initialize a shared SparkContext
conf = SparkConf().setAppName("SharedSparkContext").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

# Define configurations for Query 4:
configs = [
    {"app_name": "Q4_CONFIG_1", "executor_cores": 1, "executor_memory": "2G"},
    {"app_name": "Q4_CONFIG_2", "executor_cores": 2, "executor_memory": "4G"},
    {"app_name": "Q4_CONFIG_3", "executor_cores": 4, "executor_memory": "8G"}
]

for config in configs:
    print(f"Starting with SparkSession: {config['app_name']}")
    
    spark_session = (
        SparkSession(sc)
        .newSession()
        .builder
        .appName(config["app_name"])
        .config("spark.executor.cores", config["executor_cores"])
        .config("spark.executor.memory", config["executor_memory"])
        .getOrCreate()
    )
    
    income_df = load_income_df(spark_session)
    census_blocks_df = load_census_blocks_df(spark_session)
    crime_data_df = load_crime_data_df(spark_session)    
    re_codes_df = load_re_codes_df(spark_session)

    query3_result_df = query3(census_blocks_df, income_df, crime_data_df)

    start_time = time.time()
    top_crimes_with_race, bottom_crimes_with_race = query4(
        query3_result_df, crime_data_df, census_blocks_df, re_codes_df
    )
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Query 4 execution time for {config['app_name']}: {execution_time:.2f} seconds")
