from sedona.spark import SedonaContext
from pyspark.sql.functions import to_timestamp, col, regexp_replace

def cleanup_crime_data(crime_data_df):
        crime_data_df = crime_data_df.withColumn("Date Rptd", to_timestamp(
            "Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
        crime_data_df = crime_data_df.withColumn("DATE OCC", to_timestamp(
            "DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
        crime_data_df = crime_data_df.withColumnRenamed("AREA ", "AREA")
        return crime_data_df

def load_crime_data_df(spark_session):
    return cleanup_crime_data(spark_session \
        .read \
        .csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/", header=True, inferSchema=True))
    
    
def load_crime_data_from_single_parquet_file(spark_session):
    return cleanup_crime_data(spark_session \
        .read \
        .parquet("s3://groups-bucket-dblab-905418150721/group3/crime_data.parquet", header=True, inferSchema=True))

    
def load_census_blocks_df(spark_session):
    sedona = SedonaContext.create(spark_session)
    census_blocks_df = sedona \
        .read \
        .format("geojson") \
        .option("multiline", "true") \
        .load("s3://initial-notebook-data-bucket-dblab-905418150721/2010_Census_Blocks.geojson") \
        .selectExpr("explode(features) as features") \
        .select("features.*")
    census_blocks_df = census_blocks_df.select([col(f"properties.{col_name}").alias(col_name) for col_name in census_blocks_df.schema["properties"].dataType.fieldNames()] + ["geometry"]) \
        .drop("properties") \
        .drop("type")
    return census_blocks_df

def load_income_df(spark_session):
    income_df = spark_session \
        .read \
        .csv("s3://initial-notebook-data-bucket-dblab-905418150721/LA_income_2015.csv", header=True, inferSchema=True)
    income_df = income_df \
        .withColumn(
            "Estimated Median Income",
            regexp_replace(col("Estimated Median Income"),
                           "[$,]", "").cast("int")
        )
    return income_df

def load_re_codes_df(spark_session):
    return spark_session \
        .read \
        .csv("s3://initial-notebook-data-bucket-dblab-905418150721/RE_codes.csv", header=True, inferSchema=True)