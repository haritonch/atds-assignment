from pyspark.sql.functions import col, count, sum as pyspark_sum
from sedona.spark import ST_Point, ST_Within, ST_Union_Aggr
from timed import timed

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
        .join(income_df.hint(community_income_strategy), la_census_comms["ZCTA10"] == income_df["Zip Code"]) \
        .hint("") \
        .withColumn("income", col("housing") * col("Estimated Median Income")) \
        .groupBy("COMM") \
        .agg(
            ST_Union_Aggr("geometry").alias("comm_geometry"),
            pyspark_sum("population").alias("comm_population"),
            pyspark_sum("income").alias("comm_income")
        )

    crime_data_df = crime_data_df.withColumn("geom", ST_Point("LON", "LAT"))

    community_income_and_crime_df = community_income_df \
        .join(crime_data_df.hint(community_income_and_crime_strategy), ST_Within(crime_data_df["geom"], community_income_df["comm_geometry"])) \
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