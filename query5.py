from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, count, avg
from sedona.spark import SedonaContext, ST_Point, ST_Distance
import timed from timed

@timed
def run_query_5(spark_session, crime_df):
    sedona = SedonaContext.create(spark_session)
    police_stations_df = spark_session.read.csv(
        "s3://initial-notebook-data-bucket-dblab-905418150721/LA_Police_Stations.csv",
        header=True, inferSchema=True
    )
    police_stations_df = police_stations_df.withColumn("station_geom", ST_Point("X", "Y"))
    crime_df = crime_df.filter((col("LAT").isNotNull()) & (col("LON").isNotNull()))
    crime_df = crime_df.withColumn("crime_geom", ST_Point("LON", "LAT"))
    
    distances_df = crime_df.crossJoin(police_stations_df) \
        .withColumn("distance", ST_Distance(col("crime_geom"), col("station_geom")))

    window_spec = Window.partitionBy("DR_NO").orderBy(col("distance"))
    closest_stations_df = distances_df.withColumn("rank", rank().over(window_spec)).filter(col("rank") == 1)

    query5_result_df = closest_stations_df.groupBy("DIVISION") \
        .agg(
            count("DR_NO").alias("crime_count"),
            avg("distance").alias("average_distance")
        ) \
        .orderBy(col("crime_count").desc())

    query5_result_df.select(
        col("DIVISION").alias("division"),
        col("average_distance"),
        col("crime_count").alias("#")
    ).orderBy(col("#").desc()).show()

    return query5_result_df