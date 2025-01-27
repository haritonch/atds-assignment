from pyspark.sql.functions import col, when
from timed import timed

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
        if int(row["Vict Age"]) < 18:
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
        .sortBy(lambda x: x[1], ascending=False) \
        .toDF(["Vict Age Group", "count"]) \
        .show()
