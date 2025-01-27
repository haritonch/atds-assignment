from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Initialize a shared SparkContext
conf = SparkConf().setAppName("SharedSparkContext").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

# Workflow for each configuration
configs = [
    {"app_name": "Q5_CONFIG_1", "num_executors": 2, "executor_cores": 4, "executor_memory": "8G"},
    {"app_name": "Q5_CONFIG_2", "num_executors": 4, "executor_cores": 2, "executor_memory": "4G"},
    {"app_name": "Q5_CONFIG_3", "num_executors": 8, "executor_cores": 1, "executor_memory": "2G"},
]

for config in configs:
    log_progress(f"Starting with SparkSession: {config['app_name']}")
    
    # Create a new SparkSession using the shared SparkContext and apply resource configurations
    spark_session = (
        SparkSession(sc)
        .newSession()
        .builder
        .appName(config["app_name"])
        .config("spark.executor.instances", config["num_executors"])
        .config("spark.executor.cores", config["executor_cores"])
        .config("spark.executor.memory", config["executor_memory"])
        .getOrCreate()
    )
    
    # Load the crime dataset
    crime_df = spark_session.read.csv(
        "s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/",
        header=True,
        inferSchema=True
    )
    log_progress("/CrimeData loaded successfully.")
    
    # Execute Query 5 with timing
    query5_result_df, execution_time = run_query_5(spark_session, crime_df)
    print(f"Query 5 execution time for {config['app_name']}: {execution_time:.2f} seconds")
