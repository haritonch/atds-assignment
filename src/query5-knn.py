from pyspark.sql.functions import col, count, avg
from sedona.spark import ST_Point, ST_KNN, ST_Distance
from timed import timed
from sedona.register import SedonaRegistrator

@timed
def query5_KNN(spark_session, crime_df):
    SedonaRegistrator.registerAll(spark_session)
    
    police_stations_df = spark_session.read.csv(
        "s3://initial-notebook-data-bucket-dblab-905418150721/LA_Police_Stations.csv",
        header=True, inferSchema=True
    )
    police_stations_df = police_stations_df.withColumn("station_geom", ST_Point("X", "Y"))
    
    crime_df = crime_df.filter((col("LAT").isNotNull()) & (col("LON").isNotNull()))
    crime_df = crime_df.withColumn("crime_geom", ST_Point("LON", "LAT"))
    
    knn_df = ST_KNN(crime_df, police_stations_df, 1, False)
    
    # Υπολογισμός της απόστασης μεταξύ του σημείου του εγκλήματος και του πλησιέστερου σταθμού
    knn_df = knn_df.withColumn("distance", ST_Distance(col("crime_geom"), col("station_geom")))
    
    query5_result_df = knn_df.groupBy("DIVISION") \
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
