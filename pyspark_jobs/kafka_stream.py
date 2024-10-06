from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, StructField

def process_kafka_stream():
    # Initialize Spark session with the Kafka package
    spark = SparkSession.builder \
        .appName("WeatherAirQualityStreamProcessing") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
        .getOrCreate()

    # Define schema for weather data
    weather_schema = StructType() \
        .add("city", StringType()) \
        .add("temperature", DoubleType()) \
        .add("humidity", IntegerType()) \
        .add("timestamp", StringType())  # Assuming the timestamp is in string format

    # Define schema for air quality data with nested fields for 'pm25', 'pm10', and 'timestamp'
    air_quality_schema = StructType([
        StructField("city", StructType([  # Nested struct for city
            StructField("name", StringType(), True)
        ]), True),
        StructField("aqi", DoubleType(), True),
        StructField("iaqi", StructType([  # Nested struct for IAQI values (pm25, pm10)
            StructField("pm25", StructType([StructField("v", DoubleType(), True)]), True),
            StructField("pm10", StructType([StructField("v", DoubleType(), True)]), True)
        ]), True),
        StructField("time", StructType([  # Nested struct for timestamp
            StructField("iso", StringType(), True)
        ]), True)
    ])

    # Read from the weather Kafka topic
    weather_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weather-data") \
        .option("startingOffsets", "earliest") \
        .load()

    # Read from the air quality Kafka topic
    air_quality_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "air-quality-data") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the weather data from Kafka
    parsed_weather_df = weather_df.select(from_json(col("value").cast("string"), weather_schema).alias("weather_data"))

    # Parse the air quality data from Kafka
    parsed_air_quality_df = air_quality_df.select(from_json(col("value").cast("string"), air_quality_schema).alias("air_quality_data"))

    # Extract fields from parsed data for weather
    extracted_weather_df = parsed_weather_df.select(
        col("weather_data.city").alias("city"),
        col("weather_data.temperature").alias("temperature"),
        col("weather_data.humidity").alias("humidity"),
        col("weather_data.timestamp").alias("timestamp")
    )

    # Extract fields from parsed data for air quality
    extracted_air_quality_df = parsed_air_quality_df.select(
        col("air_quality_data.city.name").alias("city"),  # Access nested 'city.name'
        col("air_quality_data.aqi").alias("aqi"),
        col("air_quality_data.iaqi.pm25.v").alias("pm25"),  # Access 'pm25.v' nested field
        col("air_quality_data.iaqi.pm10.v").alias("pm10"),  # Access 'pm10.v' nested field
        col("air_quality_data.time.iso").alias("timestamp")  # Access 'time.iso' nested field
    )

    # Output the extracted weather data to the console for debugging
    extracted_weather_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Output the extracted air quality data to the console for debugging
    extracted_air_quality_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Transformation example: Calculate the average temperature for each city (just for weather data)
    avg_weather_df = extracted_weather_df.groupBy("city") \
        .agg({"temperature": "avg"}) \
        .withColumnRenamed("avg(temperature)", "avg_temperature")

    # Output the transformation result to the console
    avg_weather_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # Transformation example: Calculate the average AQI for each city (just for air quality data)
    avg_air_quality_df = extracted_air_quality_df.groupBy("city") \
        .agg({"aqi": "avg"}) \
        .withColumnRenamed("avg(aqi)", "avg_aqi")

    # Output the transformation result to the console
    avg_air_quality_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    # Example: Write processed weather data to SQLite
    avg_weather_df.writeStream \
        .outputMode("complete") \
        .format("jdbc") \
        .option("url", "jdbc:sqlite:api/data.db") \
        .option("dbtable", "weather_aggregates") \
        .option("checkpointLocation", "/path/to/checkpoints/weather") \
        .start()

# Example: Write processed air quality data to SQLite
    avg_air_quality_df.writeStream \
        .outputMode("complete") \
        .format("jdbc") \
        .option("url", "jdbc:sqlite:api/data.db") \
        .option("dbtable", "air_quality_aggregates") \
        .option("checkpointLocation", "/path/to/checkpoints/air_quality") \
        .start()

    # Wait for the streaming queries to finish
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_kafka_stream()
