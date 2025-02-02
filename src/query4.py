from pyspark.sql.functions import col, udf, broadcast
from pyspark.sql.types import StringType
from sedona.spark import ST_Point, ST_Within
from timed import timed

@timed
def query4(query3_result_df, crime_data_df, census_blocks_df, race_codes_df):
    spark_session = crime_data_df.sql_ctx.sparkSession

    top_3_areas = query3_result_df.orderBy(col("Income per capita").desc()).limit(3)
    bottom_3_areas = query3_result_df.orderBy(col("Income per capita").asc()).limit(3)

    top_comms = [row.COMM for row in top_3_areas.select("COMM").collect()]
    bottom_comms = [row.COMM for row in bottom_3_areas.select("COMM").collect()]

    crime_data_with_geom = crime_data_df.withColumn("crime_geom", ST_Point("LON", "LAT"))

    # Επιλογή μόνο των απαραίτητων στηλών από το census_blocks_df και broadcast
    census_blocks_small = census_blocks_df.select("COMM", "geometry")
    crimes_with_comm = crime_data_with_geom.join(
        broadcast(census_blocks_small),
        ST_Within(col("crime_geom"), col("geometry")),
        "left"
    ).cache()  # caching, καθώς θα χρησιμοποιηθεί για δύο φίλτρα

    # Φιλτράρισμα εγκλημάτων για τις Top και Bottom κοινότητες
    top_crimes = crimes_with_comm.filter(col("COMM").isin(top_comms))
    bottom_crimes = crimes_with_comm.filter(col("COMM").isin(bottom_comms))

    race_mapping = {row["Vict Descent"]: row["Vict Descent Full"] for row in race_codes_df.collect()}
    broadcast_race_mapping = spark_session.sparkContext.broadcast(race_mapping)

    def map_race(code):
        return broadcast_race_mapping.value.get(code, None)
    map_race_udf = udf(map_race, StringType())

    # Εφαρμογή του UDF στα DataFrame για να προσθέσουμε την πλήρη περιγραφή της φυλής
    top_crimes_with_race = top_crimes.withColumn("Vict Descent Full", map_race_udf(col("Vict Descent")))
    bottom_crimes_with_race = bottom_crimes.withColumn("Vict Descent Full", map_race_udf(col("Vict Descent")))

    # Ομαδοποίηση και υπολογισμός αριθμού εγκλημάτων ανά race profile
    top_grouped = top_crimes_with_race.groupBy("Vict Descent Full") \
                                      .count() \
                                      .orderBy(col("count").desc())
    bottom_grouped = bottom_crimes_with_race.groupBy("Vict Descent Full") \
                                            .count() \
                                            .orderBy(col("count").desc())

    print("Race profile για τις Top 3 Κοινότητες (ανάλογα με Income per capita):")
    top_grouped.show()

    print("Race profile για τις Bottom 3 Κοινότητες (ανάλογα με Income per capita):")
    bottom_grouped.show()

    return top_crimes_with_race, bottom_crimes_with_race
