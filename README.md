# Προχωρημένα Θέματα Βάσεων Δεδομένων
## Ακ. έτος 2024-2025

### Group 3
- Συρμακέζης Χρήστος: `03115182`
- Χαρίτων Χαριτωνίδης: `03116694`

[GitHub Repository](https://github.com/haritonch/atds-assignment) 

---

## Ζητούμενο 1

### Περιγραφή
Να υλοποιηθεί το Query 1 χρησιμοποιώντας τα DataFrame και RDD APIs. Να εκτελέσετε και τις δύο υλοποιήσεις με 4 Spark executors. 
Υπάρχει διαφορά στην επίδοση μεταξύ των δύο APIs;

### Κώδικας

```python
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
```

```python
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
        .sortBy(lambda x: -x[1]) \
        .toDF(["Vict Age Group", "count"]) \
        .show()
```

### Αποτελέσματα

|Vict Age Group| count|
|--------------|------|
|         25-64|115298|
|         18-24| 32197|
|           <18| 15321|
|           >64|  5676|

Ο χρόνος εκτέλεσης με Dataframe API ήταν 1.5s ενώ με RDD ήταν 13.5s.

### Συμπεράσματα
- Το DataFrame API είναι γενικά ταχύτερο λόγω του Catalyst Optimizer.
- Το RDD API απαιτεί χειροκίνητη βελτιστοποίηση.

---

## Ζητούμενο 2
### Περιγραφή
α) Να υλοποιηθεί το Query 2 χρησιμοποιώντας τα DataFrame και SQL APIs. Να αναφέρετε και να συγκρίνετε τους χρόνους εκτέλεσης μεταξύ των δύο υλοποιήσεων.

β) Να γράψετε κώδικα Spark που μετατρέπει το κυρίως data set σε Parquet file format και αποθηκεύει ένα μοναδικό .parquet αρχείο στο S3 bucket της ομάδας σας. Επιλέξτε μία από τις δύο υλοποιήσεις του υποερωτήματος α) (DataFrame ή SQL) και συγκρίνετε τους χρόνους εκτέλεσης της εφαρμογής σας όταν τα δεδομένα εισάγονται σαν .csv και σαν .parquet.

### Κώδικας

#### SQL
```python
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
```

#### DataFrame
```python
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
```

### Αποτελέσματα

Οι πρώτες 20 γραμμές του αποτελέσματος είναι:

|year|   precinct|    closed_case_rate|  #|
|----|-----------|--------------------|---|
|2010|    Rampart| 0.32847134489491214|  1|
|2010|    Olympic|  0.3151528982199909|  2|
|2010|     Harbor|  0.2936028339237341|  3|
|2010|West Valley| 0.25979070018340705|  4|
|2010| Hollenbeck|  0.2520928976505536|  5|
|2010|  Northeast|  0.2470599303791514|  6|
|2010|  Southeast| 0.24034296028880867|  7|
|2010|     Newton| 0.23422342234223423|  8|
|2010|   Van Nuys| 0.21825750817722273|  9|
|2010|N Hollywood| 0.20818301803783545| 10|
|2010|  Southwest|  0.1888270610631132| 11|
|2010|  Hollywood|  0.1694896851248643| 12|
|2010|    Central| 0.16946543520850826| 13|
|2010|   Wilshire| 0.15355484974346445| 14|
|2010|    Pacific| 0.09528531506355359| 15|
|2010|    Topanga| 0.06825284806936183| 16|
|2010|   Foothill|0.056363636363636366| 17|
|2010|    Mission|0.053777693618632834| 18|
|2010|    West LA| 0.04719512195121951| 19|
|2010| Devonshire|0.024433793816370643| 20|

- Xρόνος εκτέλεσης με RDD API: 12.5s
- Χρόνος εκτέλεσης με SQL API: 1.4s
- Τρέχοντας το ίδιο query φορτώνοντας το dataframe από ένα μόνο parquet file ο χρόνος εκτέλεσης με RDD API γίνεται 19.5s


### Συμπεράσματα
- Το SQL API είναι ταχύτερο για σύνθετα queries λόγω του Catalyst Optimizer.
- Για να επιτύχουμε την ίδια επίδοση με RDD απαιτείται βελτιστοποιημένος χειροκίνητος κώδικας.
- Η εξήγηση που δίνουμε στο ότι ο χρόνος έγινε χειρότερος με το μοναδικό parquet file είναι ότι το spark διανέμει καλύτερα τα δεδομένα ανάμεσα στους workers όταν φορτώνονται από πολλά αρχεία και επιτυγχάνεται καλύτερος παραλληλισμός.

---

## Ζητούμενο 3

### Περιγραφή
Να υλοποιηθεί το Query 3 χρησιμοποιώντας DataFrame ή SQL API. Χρησιμοποιήστε τις μεθόδους `hint` & `explain` για να βρείτε ποιες στρατηγικές join χρησιμοποιεί ο Catalyst Optimizer.

Πειραματιστείτε αναγκάζοντας το Spark να χρησιμοποιήσει διαφορετικές στρατηγικές (μεταξύ των BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL) και σχολιάστε τα αποτελέσματα που παρατηρείτε.

### Κώδικας

```python
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
```

### Στρατηγικές

```python
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
```

### Αποτελέσματα
Δυστυχώς δεν έχουμε αποτελέσματα καθώς δεν προλάβαμε να τα σώσουμε πριν πέσει το σύστημα.

### Αναμενόμενα Συμπεράσματα
- **BROADCAST**: Ιδανικό για **μικρά** datasets.
- **MERGE**: Αποτελεσματικό για **μεγάλα**, **ταξινομημένα** datasets.
- **SHUFFLE_HASH**: Κατάλληλο για datasets μεσαίου μεγέθους.
---

## Ζητούμενο 4

### Περιγραφή
Να υλοποιηθεί το Query 4 χρησιμοποιώντας το DataFrame ή SQL API. Να εκτελέσετε την υλοποίησή σας εφαρμόζοντας κλιμάκωση στο σύνολο των υπολογιστικών πόρων που θα χρησιμοποιήσετε.

Συγκεκριμένα, καλείστε να εκτελέστε την υλοποίησή σας σε 2 executors με τα ακόλουθα configurations:
- 1 core / 2 GB memory
- 2 cores / 4 GB memory
- 4 cores / 8 GB memory

### Κώδικας

```python
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
```

### Αποτελέσματα
Out of resources

### Συμπεράσματα
Out of resources

---

## Ζητούμενο 5

### Περιγραφή
Να υλοποιηθεί το Query 5 χρησιμοποιώντας το DataFrame ή SQL API. Να εκτελέσετε την υλοποίησή σας χρησιμοποιώντας συνολικούς πόρους 8 cores και 16 GB μνήμης με τα παρακάτω configurations:
- 2 executors × 4 cores / 8 GB memory
- 4 executors × 2 cores / 4 GB memory
- 8 executors × 1 core / 2 GB memory

### Κώδικας

```python
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
```



### Αποτελέσματα

| Division          | Average Distance     | Count   |
|-------------------|-----------------------|---------|
| HOLLYWOOD         | 0.02043779072548565  | 213080  |
| VAN NUYS          | 0.02865315459062913  | 211457  |
| WILSHIRE          | 0.026312166557481583 | 198150  |
| SOUTHWEST         | 0.02157700118424315  | 186742  |
| OLYMPIC           | 0.01729162112331338  | 180463  |
| NORTH HOLLYWOOD   | 0.02611521422256773  | 171159  |
| 77TH STREET       | 0.01658487149606819  | 167323  |
| PACIFIC           | 0.037495777088312074 | 157468  |
| CENTRAL           | 0.0098680868492353   | 154474  |
| SOUTHEAST         | 0.02415012719550646  | 151999  |


- `Q5_CONFIG_1`: 32.7662 sec
- `Q5_CONFIG_2`: 26.7681 sec
- `Q5_CONFIG_3`: 23.6621s sec


### Συμπεράσματα
- **Παραλληλία**: Η αύξηση των executors μειώνει σημαντικά τον χρόνο εκτέλεσης, λόγω καλύτερης κατανομής του φόρτου εργασίας.
- **Μνήμη**: Μικρότερη μνήμη ανά executor δεν επηρεάζει την απόδοση, εφόσον τα δεδομένα χωρούν στους διαθέσιμους πόρους.
- **Cores/Executor**: Περισσότεροι executors με λιγότερα cores οδήγησαν σε ταχύτερη επεξεργασία.

> *Η παραλληλία είναι πιο σημαντική από την ισχύ κάθε εκτελεστή*

**Καλύτερο Configuration**: Το `Q5_CONFIG_3` είναι το πιο αποδοτικό, με την καλύτερη ισορροπία παραλληλίας και πόρων.


### Benchmark Script

```python
# Initialize a shared SparkContext
conf = SparkConf().setAppName("SharedSparkContext").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

# Workflow for each configuration
configs = [
    {"app_name": "Q5_CONFIG_1", "num_executors": 2, "executor_cores": 4, "executor_memory": "8G"},
    {"app_name": "Q5_CONFIG_2", "num_executors": 4, "executor_cores": 2, "executor_memory": "4G"},
    {"app_name": "Q5_CONFIG_3", "num_executors": 8, "executor_cores": 1, "executor_memory": "2G"},
]

for config in configs:
    log_progress(f"Starting with SparkSession: {config['app_name']}")
    
    # Create a new SparkSession using the shared SparkContext and apply resource configurations
    spark_session = (
        SparkSession(sc)
        .newSession()
        .builder
        .appName(config["app_name"])
        .config("spark.executor.instances", config["num_executors"])
        .config("spark.executor.cores", config["executor_cores"])
        .config("spark.executor.memory", config["executor_memory"])
        .getOrCreate()
    )
    
    crime_df = spark_session.read.csv(
        "s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/",
        header=True,
        inferSchema=True
    )
    log_progress("/CrimeData loaded successfully.")
    
    # Measure Q5 perfomance for each config
    query5_result_df, execution_time = run_query_5(spark_session, crime_df)
    print(f"Query 5 execution time for {config['app_name']}: {execution_time:.2f} seconds")
```