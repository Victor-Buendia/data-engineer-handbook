# %%
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("SparkHomework")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.debug.maxToStringFields", "1000")
    .config("spark.sql.iceberg.planning.distribution-mode", "hash")
    .appName("SparkIcebergOptimized")
    # Tell Spark to use the same number of partitions as our buckets
    .config("spark.sql.shuffle.partitions", "16") 
    # --- Configuration for REST Catalog ---
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "rest")
    .config("spark.sql.catalog.spark_catalog.uri", "http://rest:8181")
    # --- NEW: S3/MinIO Configuration for Spark and Iceberg ---
    .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.spark_catalog.s3.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

# %%
# %%
# Forcefully set the S3 configuration on the underlying Hadoop configuration
# This can sometimes fix issues where SparkConf doesn't propagate correctly.

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# %%
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")

maps.createOrReplaceTempView("maps")
matches.createOrReplaceTempView("matches")

# %%
%%sql
SELECT * FROM maps

# %%
%%sql
SELECT * FROM matches

# %%
from pyspark.sql.functions import broadcast

matchesAndMaps = (
    matches.join(
        broadcast(maps),
        on=[matches.mapid == maps.mapid],
        how="inner"
    )
)
matchesAndMaps.createOrReplaceTempView("matchesAndMaps")

matchesAndMapsPlan = (
    matches.join(
        broadcast(maps),
        on=[matches.mapid == maps.mapid],
        how="inner"
    )
).explain()

# %%
%%sql
SELECT COUNT(1) FROM matchesAndMaps

# %%
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
medal_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")

# %%
medal_matches_players.printSchema()

# %%
%%sql
CREATE DATABASE IF NOT EXISTS spark_catalog.bootcamp;

# %%
%%sql
DROP TABLE IF EXISTS spark_catalog.bootcamp.matchesBucketed;

# %%
%%sql
CREATE TABLE IF NOT EXISTS spark_catalog.bootcamp.matchesBucketed (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over BOOLEAN,
    completion_date TIMESTAMP,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
TBLPROPERTIES (
    'write.sort-order'='match_id ASC',
    'write.distribution-mode'='hash'
);

# %%
%%sql
DROP TABLE IF EXISTS spark_catalog.bootcamp.matchDetailsBucketed;

# %%
%%sql
CREATE TABLE IF NOT EXISTS spark_catalog.bootcamp.matchDetailsBucketed (
    match_id STRING,
    player_gamertag STRING,
    previous_spartan_rank INT,
    spartan_rank INT,
    previous_total_xp INT,
    total_xp INT,
    previous_csr_tier INT,
    previous_csr_designation INT,
    previous_csr INT,
    previous_csr_percent_to_next_tier INT,
    previous_csr_rank INT,
    current_csr_tier INT,
    current_csr_designation INT,
    current_csr INT,
    current_csr_percent_to_next_tier INT,
    current_csr_rank INT,
    player_rank_on_team INT,
    player_finished BOOLEAN,
    player_average_life STRING,
    player_total_kills INT,
    player_total_headshots INT,
    player_total_weapon_damage DOUBLE,
    player_total_shots_landed INT,
    player_total_melee_kills INT,
    player_total_melee_damage DOUBLE,
    player_total_assassinations INT,
    player_total_ground_pound_kills INT,
    player_total_shoulder_bash_kills INT,
    player_total_grenade_damage DOUBLE,
    player_total_power_weapon_damage DOUBLE,
    player_total_power_weapon_grabs INT,
    player_total_deaths INT,
    player_total_assists INT,
    player_total_grenade_kills INT,
    did_win INT,
    team_id INT
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
TBLPROPERTIES (
    'write.sort-order'='match_id ASC',
    'write.distribution-mode'='hash'
);

# %%
%%sql
DROP TABLE IF EXISTS spark_catalog.bootcamp.medalsMatchesPlayersBucketed;

# %%
%%sql
CREATE TABLE IF NOT EXISTS spark_catalog.bootcamp.medalsMatchesPlayersBucketed (
    match_id STRING,
    player_gamertag STRING,
    medal_id BIGINT,
    count INT
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
TBLPROPERTIES (
    'write.sort-order'='match_id ASC',
    'write.distribution-mode'='hash'
);

# %%
matchDetailsBucketed = (
    match_details
    .write
    .bucketBy(16, "match_id")
    # .sortBy("match_id")
    .format("iceberg")
    .mode("overwrite")
    .saveAsTable("spark_catalog.bootcamp.matchDetailsBucketed")
)

matchesBucketed = (
    matches
    .write
    .bucketBy(16, "match_id")
    # .sortBy("match_id")
    .format("iceberg")
    .mode("overwrite")
    .saveAsTable("spark_catalog.bootcamp.matchesBucketed")
)

medalsMatchesPlayersBucketed = (
    medal_matches_players
    .write
    .bucketBy(16, "match_id")
    # .sortBy("match_id")
    .format("iceberg")
    .mode("overwrite")
    .saveAsTable("spark_catalog.bootcamp.medalsMatchesPlayersBucketed")
)

# %%
matchDetailsBucketed = spark.table("spark_catalog.bootcamp.matchDetailsBucketed")
matchesBucketed = spark.table("spark_catalog.bootcamp.matchesBucketed")
medalsMatchesPlayersBucketed = spark.table("spark_catalog.bootcamp.medalsMatchesPlayersBucketed")

# %%
# Join match_details, matches, and medal_matches_players on 'match_id'
joined_df = (
    matchDetailsBucketed
    .join(matchesBucketed, on="match_id", how="inner")
    .join(medalsMatchesPlayersBucketed, on="match_id", how="inner")
).explain()

joined_df = (
    matchDetailsBucketed
    .join(matchesBucketed, on="match_id", how="inner")
    .join(medalsMatchesPlayersBucketed, on="match_id", how="inner")
)

# %%
joined_df.show(5)
joined_df.createOrReplace("joined_df")

# %%
from pyspark.sql.functions import col, avg, desc, count

player_avg_kills = (
    joined_df
    .groupBy("player_gamertag")
    .agg(avg("player_total_kills").alias("average_kills"))
    .orderBy(desc("average_kills"))
)
player_avg_kills.show(5)

# %%
most_played_playlist = (
    joined_df
    .groupBy("playlist_name")
    .agg(count("*").alias("play_count"))
    .orderBy(desc("play_count"))
)
most_played_playlist.show(5)

# %%
most_played_map = (
    joined_df
    .groupBy("name") # 'name' is the map name column from the maps table
    .agg(count("*").alias("play_count"))
    .orderBy(desc("play_count"))
)
most_played_map.show(5)

# %%
killing_spree_map = (
    joined_df
    .filter(col("name_medal") == "Killing Spree")
    .groupBy("name") # 'name' is the map name
    .agg(count("*").alias("killing_spree_count"))
    .orderBy(desc("killing_spree_count"))
)
killing_spree_map.show(5)

# %%
partitioned_df = joined_df.repartition(8)
sorted_by_playlist = partitioned_df.sortWithinPartitions("playlist_id")
sorted_by_map = partitioned_df.sortWithinPartitions("name")

# %%
%%sql
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files FROM spark_catalog.bootcamp.joined_df.files;


