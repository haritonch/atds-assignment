from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, when, col, to_timestamp, year, rank, count, isnan, regexp_replace, trim, sum as pyspark_sum
from pyspark.sql.window import Window
from sedona.spark import SedonaContext, ST_Point, ST_Within, ST_Union_Aggr
import time
from datetime import datetime


def timed(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(
            f"Execution time for {func.__name__}: {end_time - start_time:.4f} seconds")
        return result
    return wrapper


def load_crime_data_df(spark_session):
    return spark_session \
        .read \
        .csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/", header=True, inferSchema=True)


def cleanup_crime_data(crime_data_df):
    crime_data_df = crime_data_df.withColumn("Date Rptd", to_timestamp(
        "Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
    crime_data_df = crime_data_df.withColumn("DATE OCC", to_timestamp(
        "DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
    crime_data_df = crime_data_df.withColumnRenamed("AREA ", "AREA")
    return crime_data_df


@timed
def query1_dataframe(crime_data_df):
    crime_data_df \
        .filter(crime_data_df["Crm Cd Desc"].contains("AGGRAVATED ASSAULT")) \
        .withColumn(
            "Vict Age Group",
            when(col("Vict Age") < 18, "<18")
            .when((18 <= col("Vict Age")) & (col("Vict Age") <= 24), "18-24")
            .when((25 <= col("Vict Age")) & (col("Vict Age") <= 64), "25-64")
            .when(64 < col("Vict Age"), ">64")) \
        .groupBy("Vict Age Group") \
        .count() \
        .orderBy("count", ascending=False) \
        .show()


@timed
def query1_rdd(crime_data_df):
    def age_group(row):
        if int(row['Vict Age']) < 18:
            return "<18"
        elif 18 <= int(row['Vict Age']) <= 24:
            return "18-24"
        elif 25 <= int(row['Vict Age']) <= 64:
            return "25-64"
        else:
            return ">64"

    crime_data_df \
        .rdd \
        .filter(lambda x: "AGGRAVATED ASSAULT" in x["Crm Cd Desc"]) \
        .map(lambda x: (age_group(x), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: -x[1]) \
        .toDF(["Vict Age Group", "count"]) \
        .show()


@timed
def query2_rdd(crime_data_df):
    def processed_indicator(row):
        if row["Status Desc"] in ("Invest Cont", "UNK"):
            return 0
        else:
            return 1

    crime_data_df \
        .rdd \
        .map(lambda row: ((row["AREA NAME"], row["DATE OCC"].year), (processed_indicator(row), 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: x[0] / x[1]) \
        .map(lambda x: (x[0][1], x[0][0], x[1])) \
        .toDF([
            "year",
            "precinct",
            "closed_case_rate",
        ]) \
        .withColumn("#", rank().over(Window.partitionBy("year").orderBy(col("closed_case_rate").desc()))) \
        .orderBy("year", "#") \
        .show()


@timed
def query2_sql(spark_session, crime_data_df):
    crime_data_df.createOrReplaceTempView("crime_data")
    query = """
        SELECT
            year,
            precinct,
            closed_case_rate,
            RANK() OVER (PARTITION BY year ORDER BY closed_case_rate DESC) as `#`
        FROM (
            SELECT 
                YEAR(`DATE OCC`) as year,
                `AREA NAME` as precinct,
                COUNT(CASE WHEN `Status Desc` NOT IN ('Invest Cont', 'UNK') THEN 1 END) / COUNT(*) as closed_case_rate
            FROM crime_data
            GROUP BY `AREA NAME`, YEAR(`DATE OCC`)
        )
    """
    result = spark_session.sql(query)
    result.show()


def save_crime_data_to_single_parquet_file(crime_data_df):
    crime_data_df \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .parquet("s3://groups-bucket-dblab-905418150721/group3/crime_data.parquet")


def load_crime_data_from_single_parquet_file(spark_session):
    return spark_session \
        .read \
        .parquet("s3://groups-bucket-dblab-905418150721/group3/crime_data.parquet")


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


@timed
def query3(census_blocks_df, income_df, crime_data_df):
    la_census_comms = census_blocks_df \
        .filter(col("CITY") == "Los Angeles") \
        .groupBy("ZCTA10", "COMM") \
        .agg(
            ST_Union_Aggr("geometry").alias("geometry"),
            pyspark_sum("HOUSING10").alias("housing"),
            pyspark_sum("POP_2010").alias("population")
        )

    community_income_df = la_census_comms \
        .join(income_df, la_census_comms["ZCTA10"] == income_df["Zip Code"]) \
        .withColumn("income", col("housing") * col("Estimated Median Income")) \
        .groupBy("COMM") \
        .agg(
            ST_Union_Aggr("geometry").alias("comm_geometry"),
            pyspark_sum("population").alias("comm_population"),
            pyspark_sum("income").alias("comm_income")
        )

    crime_data_df = crime_data_df.withColumn("geom", ST_Point("LON", "LAT"))

    community_income_and_crime_df = community_income_df \
        .join(crime_data_df, ST_Within(crime_data_df["geom"], community_income_df["comm_geometry"])) \
        .groupBy("COMM", "comm_population", "comm_income") \
        .agg(
            count(col("*")).alias("#crimes"),
        ) \
        .withColumn("#Crimes per capita", col("#crimes") / col("comm_population")) \
        .withColumn("Income per capita", col("comm_income") / col("comm_population")) \
        .select("COMM", "#Crimes per capita", "Income per capita") \
        .orderBy(col("Income per capita").desc())

    community_income_and_crime_df.show()
    return community_income_and_crime_df


@timed
def query3_with_strategies(
        census_blocks_df,
        income_df,
        crime_data_df,
        community_income_strategy,
        community_income_and_crime_strategy):
    la_census_comms = census_blocks_df \
        .filter(col("CITY") == "Los Angeles") \
        .groupBy("ZCTA10", "COMM") \
        .agg(
            ST_Union_Aggr("geometry").alias("geometry"),
            pyspark_sum("HOUSING10").alias("housing"),
            pyspark_sum("POP_2010").alias("population")
        )

    community_income_df = la_census_comms \
        .join(income_df, la_census_comms["ZCTA10"] == income_df["Zip Code"]) \
        .withColumn("income", col("housing") * col("Estimated Median Income")) \
        .groupBy("COMM") \
        .agg(
            ST_Union_Aggr("geometry").alias("comm_geometry"),
            pyspark_sum("population").alias("comm_population"),
            pyspark_sum("income").alias("comm_income")
        )

    crime_data_df = crime_data_df.withColumn("geom", ST_Point("LON", "LAT"))

    community_income_and_crime_df = community_income_df \
        .join(crime_data_df, ST_Within(crime_data_df["geom"], community_income_df["comm_geometry"])) \
        .groupBy("COMM", "comm_population", "comm_income") \
        .agg(
            count(col("*")).alias("#crimes"),
        ) \
        .withColumn("#Crimes per capita", col("#crimes") / col("comm_population")) \
        .withColumn("Income per capita", col("comm_income") / col("comm_population")) \
        .select("COMM", "#Crimes per capita", "Income per capita") \
        .orderBy(col("Income per capita").desc())

    community_income_and_crime_df.show()
    return community_income_and_crime_df

def load_re_codes_df(spark_session):
    return spark_session \
        .read \
        .csv("s3://initial-notebook-data-bucket-dblab-905418150721/RE_codes.csv", header=True, inferSchema=True)

@timed
def query4(query3_result_df, crime_data_df, race_codes_df):
    top_3_areas = query3_result_df.orderBy(col("Income per capita").desc()).limit(3)
    bottom_3_areas = query3_result_df.orderBy(col("Income per capita").asc()).limit(3)
    
    top3_areas_crimes = crime_2015_df.join(top_3_areas, "COMM")
    bottom3_areas_crimes = crime_2015_df.join(bottom_3_areas, "COMM")
    
    top3_areas_crimes_with_re_codes = top3_areas_crimes.join(race_codes_df, top3_areas_crimes["Vict Descent"] == race_codes_df["DescentCode"], "left")
    bottom3_areas_crimes_with_re_codes = bottom3_areas_crimes.join(race_codes_df, bottom3_areas_crimes["Vict Descent"] == race_codes_df["DescentCode"], "left")
    
    top3_areas_crimes_with_re_codes.groupBy("Vict Descent Full").count()
    bottom3_areas_crimes_with_re_codes.groupBy("Vict Descent Full").count()
    
    top3_areas_crimes_with_re_codes.show()
    bottom3_areas_crimes_with_re_codes.show()
    
    return top3_areas_crimes_with_re_codes, bottom3_areas_crimes_with_re_codes
    

def main():
    print("Initializing spark session")
    spark_session = SparkSession \
        .builder \
        .appName("LosAngelesCrime") \
        .getOrCreate()

    print("Loading crime data dataframe")
    crime_data_df = load_crime_data_df(spark_session)
    crime_data_df = cleanup_crime_data(crime_data_df)

    # Q1
    query1_dataframe(crime_data_df)
    query1_rdd(crime_data_df)

    # Q2
    print("Running query 2 RDD")
    query2_rdd(crime_data_df)
    print("Running query 2 SQL")
    query2_sql(spark_session, crime_data_df)
    print("Saving crime data to a single parquet file")
    save_crime_data_to_single_parquet_file(crime_data_df)
    print("Loading crime data from single parquet file")
    parquet_loaded_crime_data_df = load_crime_data_from_single_parquet_file(
        spark_session)
    parquet_loaded_crime_data_df = cleanup_crime_data(
        parquet_loaded_crime_data_df)
    print("Running query 2 RDD again with the dataframe being loaded from a single parquet file")
    query2_rdd(parquet_loaded_crime_data_df)

    # Q3
    print("Loading income dataframe")
    income_df = load_income_df(spark_session)
    print("Loading census blocks dataframe")
    census_blocks_df = load_census_blocks_df(spark_session)
    print("Running query 3")
    query3_result_df = query3(census_blocks_df, income_df, crime_data_df)
    print("Explaining query 3 result")
    query3_result_df.explain()
    print("Running query 3 with various strategies")
    # TODO: hint etc.
    
    # Q4
    re_codes_df = load_re_codes_df(spark_session)
    query4(query3_result_df, crime_data_df, re_codes_df)
        

if __name__ == "__main__":
    main()
