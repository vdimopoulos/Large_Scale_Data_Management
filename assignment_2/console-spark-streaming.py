from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    FloatType,
    StringType,
    TimestampType,
)
from pyspark.sql.functions import split, from_json, col, lpad
from pyspark.sql.functions import to_timestamp

songSchema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("user_name", StringType(), False),
        StructField("song", StringType(), False),
        StructField("time", StringType(), False),
    ]
)

csvSchema = StructType(
    [
        StructField("song", StringType(), False),
        StructField("artists", StringType(), False),
        StructField("duration_ms", IntegerType(), False),
        StructField("album_name", StringType(), False),
        StructField("album_release_date", StringType(), False),
        StructField("danceability", FloatType(), False),
        StructField("energy", FloatType(), False),
        StructField("key", IntegerType(), False),
        StructField("loudness", FloatType(), False),
        StructField("mode", IntegerType(), False),
        StructField("speechiness", FloatType(), False),
        StructField("acousticness", FloatType(), False),
        StructField("instrumentalness", FloatType(), False),
        StructField("liveness", FloatType(), False),
        StructField("valence", FloatType(), False),
        StructField("tempo", FloatType(), False),
    ]
)

spark = (
    SparkSession.builder.appName("SSKafka")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "test")
    .option("startingOffsets", "latest")
    .load()
)

sdf = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), songSchema).alias("data"))
    .select("data.*")
)

sdf = sdf.withColumn("year", col("time").cast("string").substr(1, 4).cast("int"))
sdf = sdf.withColumn("month", col("time").cast("string").substr(9, 2).cast("int"))
sdf = sdf.withColumn("day", col("time").cast("string").substr(6, 2).cast("int"))
sdf = sdf.withColumn("hour", col("time").cast("string").substr(12, 2).cast("int"))
sdf = sdf.withColumn("minute", col("time").cast("string").substr(15, 2).cast("int"))
sdf = sdf.withColumn("second", col("time").cast("string").substr(18, 2).cast("int"))
sdf = sdf.drop("time")

csv_df = spark.read.csv("spotify-songs.csv", header=True, schema=csvSchema)
enriched_df = sdf.join(csv_df, "song", "left_outer")

enriched_df.writeStream.outputMode("update").format("console").option(
    "truncate", False
).start().awaitTermination()
