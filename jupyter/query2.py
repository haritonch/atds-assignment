from pyspark.sql.functions import rank, col
from pyspark.sql.window import Window
from timed import timed


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


def save_crime_data_to_single_parquet_file(crime_data_df):
    crime_data_df \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .parquet("/home/data/crime_data.parquet")


def load_crime_data_from_single_parquet_file(spark_session):
    return spark_session \
        .read \
        .parquet("/home/crime_data.parquet")
