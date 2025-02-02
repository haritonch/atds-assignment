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

Ο χρόνος εκτέλεσης με Dataframe API ήταν 10.0s ενώ με RDD ήταν 27.9s.

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

- Xρόνος εκτέλεσης με RDD API: 9.5s
- Χρόνος εκτέλεσης με SQL API: 3.3s
- Τρέχοντας το ίδιο query φορτώνοντας το dataframe από ένα μόνο parquet file ο χρόνος εκτέλεσης με RDD API γίνεται 46.3s


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
Το αποτέλεσμα του query είναι: (20 πρώτες γραμμές)

|               COMM| #Crimes per capita| Income per capita|
|-------------------|-------------------|------------------|
|  Pacific Palisades|0.45085501138400425| 70656.11282274863|
|Palisades Highlands| 0.2055830941821028| 66867.43986433603|
|   Marina Peninsula| 0.6124048881715471| 65235.69402813004|
|            Bel Air| 0.4275511439293064| 63041.33809466166|
|      Beverly Crest| 0.3713395127553113| 60947.49019768682|
|          Brentwood| 0.5036005597078598|60840.624859219824|
|  Mandeville Canyon|0.26229508196721313| 55572.10949582431|
|        Playa Vista| 0.8157069235939951| 50264.47187990141|
|            Carthay| 0.9322445879225219|49841.164527155335|
|             Venice| 1.2549272030651342|47614.883340996166|
|       Century City| 0.8558452481076535|46103.510597140456|
|      Playa Del Rey| 0.7770740975300824|   45522.596580114|
|        Studio City| 0.9295754238516157| 44049.85263005362|
|    Hollywood Hills| 0.7937264742785446|43015.988205771646|
|      South Carthay|  0.723174477360547|39831.340037649854|
|   West Los Angeles| 0.8311722523049905| 39714.00529781941|
|       Miracle Mile| 0.8820452139253908| 38834.84611073052|
|        Rancho Park| 1.2552819698173154|38740.063860206516|
|             Encino|  0.703747432052705| 38338.00564358072|
|       Sherman Oaks| 0.7903687241383545|37767.445673661336|


Και ο χρόνος εκτέλεσης ήταν 23.9s

Οι χρόνοι εκτέλεσης σε συνάρτηση με τις στρατηγικές join φαίνονται στον παρακάτω πίνακα:

| Community/Income Join strategy | Community income / Crime strategy | Execution Time (s) |
|--------------------------------|-----------------------------------|--------------------|
| BROADCAST                      | BROADCAST                         | crashes            |
| BROADCAST                      | MERGE                             | 18.5               |
| BROADCAST                      | SHUFFLE_HASH                      | 18.4               |
| BROADCAST                      | SHUFFLE_REPLICATE_NL              | 41.4               |
| MERGE                          | BROADCAST                         | crashes            |
| MERGE                          | MERGE                             | 41.4               |
| MERGE                          | SHUFFLE_HASH                      | 41.2               |
| MERGE                          | SHUFFLE_REPLICATE_NL              | 40.5               |
| SHUFFLE_HASH                   | BROADCAST                         | crashes            |
| SHUFFLE_HASH                   | MERGE                             | 20.2               |
| SHUFFLE_HASH                   | SHUFFLE_HASH                      | 37.4               |
| SHUFFLE_HASH                   | SHUFFLE_REPLICATE_NL              | 37.3               |
| SHUFFLE_REPLICATE_NL           | BROADCAST                         | crashes            |
| SHUFFLE_REPLICATE_NL           | MERGE                             | 21.8               |
| SHUFFLE_REPLICATE_NL           | SHUFFLE_HASH                      | 24.3               |
| SHUFFLE_REPLICATE_NL           | SHUFFLE_REPLICATE_NL              | 18.2               |

Είναι αναμενόμενο το πρόγραμμα να "σκάει" όταν χρησιμοποιούμε broadcast στρατηγική 
για το δεύτερο join καθώς είναι μεγαλύτερο το crime dataframe και οι πόροι περιορίζονται από
το aws. Έχουμε τις εξής παρατηρήσεις:
- όταν η στρατηγική του πρώτου join είναι merge, τότε η επίδοση πέφτει. Αυτό γίνεται επειδή τα δεδομένα δεν είναι ταξινομημένα ως προς το join key ()
- Για τους υπόλοιπους συνδυασμούς, οι χρόνοι εκτέλεσης είναι περίπου ομοιόμορφοι

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
@timed
def query4(query3_result_df, crime_data_df, census_blocks_df, race_codes_df):
    # Λήψη του SparkSession από το context του DataFrame
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

    top_crimes_with_race = top_crimes.withColumn("Vict Descent Full", map_race_udf(col("Vict Descent")))
    bottom_crimes_with_race = bottom_crimes.withColumn("Vict Descent Full", map_race_udf(col("Vict Descent")))

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
```

### Αποτελέσματα

#### Race profile για τις Top 3 Κοινότητες (ανάλογα με Income per capita):

| Vict Descent Full          | count |
|----------------------------|-------|
| White                      | 8429  |
| Other                      | 1125  |
| Hispanic/Latin/Me...       | 868   |
| NULL                       | 784   |
| Unknown                    | 651   |
| Black                      | 462   |
| Other Asian                | 314   |
| Chinese                    | 42    |
| Japanese                   | 20    |
| Filipino                   | 18    |
| Korean                     | 18    |
| American Indian/A...       | 7     |
| AsianIndian                | 6     |
| Vietnamese                 | 5     |
| Hawaiian                   | 2     |

---

#### Race profile για τις Bottom 3 Κοινότητες (ανάλογα με Income per capita):

| Vict Descent Full          | count |
|----------------------------|-------|
| Hispanic/Latin/Me...       | 47026 |
| Black                      | 17151 |
| NULL                       | 12453 |
| White                      | 7265  |
| Other                      | 3256  |
| Unknown                    | 2865  |
| Other Asian                | 1979  |
| American Indian/A...       | 299   |
| Chinese                    | 145   |
| Korean                     | 103   |
| Filipino                   | 56    |
| Japanese                   | 27    |
| Vietnamese                 | 23    |
| AsianIndian                | 22    |
| Hawaiian                   | 14    |
| Pacific Islander           | 9     |
| Cambodian                  | 8     |
| Guamanian                  | 7     |
| Laotian                    | 7     |
| Samoan                     | 2     |

---

- `Q4_CONFIG_1`: 1 core / 2GB memory 77.3s
- `Q4_CONFIG_2`: 2 cores / 4GB memory: 36s
- `Q4_CONFIG_3`: 4 cores / 8GB memory: 31.8s


### Συμπεράσματα
- Παρατηρούμε ότι η αργή εκτέλεση οφείλεται κυρίως στην απουσία παράλληλης επεξεργασίας, η οποία καθιστά τα σταθερά overheads (πχ broadcasting, caching, UDF) πιο αισθητά.

- Βάζοντας τουλάχιστον 2 πυρήνες βλέπουμε μεγάλη διαφορά, ενώ με πάνω από 2, η διαφορά είναι ναι μεν αισθητή αλλά όχι τόσο εμφανής.

- Με τη χρήση ενός dictionary ως broadcast variable για τα race codes στέλνεται μία φορα σε κάθε executor και έχουμε γρήγορα lookups (πχ οταν χρησιμοποιούμε μια udf)

- Χρήσιμο για μικρά datasets / dictionaries (race codes), μειώνει το data shuffling και εξασφαλίζει ότι είναι γρήγορα και εύκολα προσβάσιμα από τα worker nodes

- Η χρήση cache διασφαλίζει οτί τα επόμενα operations δεν θα ξανάκανουν spatial join (ST_Within)

---

## Ζητούμενο 5

### Περιγραφή
Να υλοποιηθεί το Query 5 χρησιμοποιώντας το DataFrame ή SQL API. Να εκτελέσετε την υλοποίησή σας χρησιμοποιώντας συνολικούς πόρους 8 cores και 16 GB μνήμης με τα παρακάτω configurations:
- 2 executors × 4 cores / 8 GB memory
- 4 executors × 2 cores / 4 GB memory
- 8 executors × 1 core / 2 GB memory

### Κώδικας

```python
def query5(crime_df, police_stations_df):
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


- `Q5_CONFIG_1`: 2 executors × 4 cores / 8 GB memory: 32.7662 sec
- `Q5_CONFIG_2`: 4 executors × 2 cores / 4 GB memory: 26.7681 sec
- `Q5_CONFIG_3`: 8 executors × 1 core / 2 GB memory: 23.6621s sec

### Συμπεράσματα
- **Parallelism**: Η αύξηση των executors μειώνει σημαντικά τον χρόνο εκτέλεσης, λόγω καλύτερης κατανομής του φόρτου εργασίας.
- **Memory**: Μικρότερη μνήμη ανά executor δεν επηρεάζει την απόδοση, εφόσον τα δεδομένα χωρούν στους διαθέσιμους πόρους.
- **Cores/Executors**: Περισσότεροι executors με λιγότερα cores οδήγησαν σε ταχύτερη επεξεργασία.

> *Ο παραλληλισμός είναι πιο σημαντικός από την ισχύ κάθε εκτελεστή*

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