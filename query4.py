from pyspark.sql.functions import col
from timed import timed


@timed
def query4(query3_result_df, crime_data_df, race_codes_df):
    top_3_areas = query3_result_df.orderBy(col("Income per capita").desc()).limit(3)
    bottom_3_areas = query3_result_df.orderBy(col("Income per capita").asc()).limit(3)
    
    top3_areas_crimes = crime_data_df.join(top_3_areas, "COMM")
    bottom3_areas_crimes = crime_data_df.join(bottom_3_areas, "COMM")
    
    top3_areas_crimes_with_re_codes = top3_areas_crimes.join(race_codes_df, top3_areas_crimes["Vict Descent"] == race_codes_df["DescentCode"], "left")
    bottom3_areas_crimes_with_re_codes = bottom3_areas_crimes.join(race_codes_df, bottom3_areas_crimes["Vict Descent"] == race_codes_df["DescentCode"], "left")
    
    top3_areas_crimes_with_re_codes.groupBy("Vict Descent Full").count()
    bottom3_areas_crimes_with_re_codes.groupBy("Vict Descent Full").count()
    
    top3_areas_crimes_with_re_codes.show()
    bottom3_areas_crimes_with_re_codes.show()
    
    return top3_areas_crimes_with_re_codes, bottom3_areas_crimes_with_re_codes