from pyspark.sql import SparkSession
from itertools import product

from query1 import *
from query2 import *
from query3 import *
from query4 import *
from query5 import *
from load import *

def main():
    print("Initializing spark session")
    spark_session = SparkSession \
        .builder \
        .appName("LosAngelesCrime") \
        .getOrCreate()

    print("Loading crime data dataframe")
    crime_data_df = load_crime_data_df(spark_session)

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
    print("Running query 3 with default strategies")
    query3_result_df = query3(census_blocks_df, income_df, crime_data_df)
    print("Explaining query 3 result with default strategies")
    query3_result_df.explain()
    strategies = ("BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL")
    for (community_income_strategy, community_income_and_crime_strategy) in product(strategies, strategies):
        print(f"Running query 3 with community_income_strategy={community_income_strategy} and community_income_and_crime_strategy={community_income_and_crime_strategy}")
        res = query3_with_strategies(census_blocks_df, income_df, crime_data_df, community_income_and_crime_strategy, community_income_and_crime_strategy)
        res.explain()
    
    
    # Q4
    print("Running query 4")
    re_codes_df = load_re_codes_df(spark_session)
    query4_result_df = query4(query3_result_df, crime_data_df, re_codes_df)
    # query4_result_df[0].show()
    
    # Q5
    print("Running query 5")    
    query5_result_df = query5(spark_session, crime_data_df)
    query5_result_df.show()

if __name__ == "__main__":
    main()
