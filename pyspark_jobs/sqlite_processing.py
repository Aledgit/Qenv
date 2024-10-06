from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, to_date, from_unixtime

def process_sqlite_data():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SQLiteDataProcessing") \
        .master("local[*]") \
        .config("spark.jars", "jars/sqlite-jdbc-3.46.1.3.jar") \
        .getOrCreate()

    # Connect to the SQLite database
    jdbc_url = "jdbc:sqlite:api/data.db"

    # Read weather data from SQLite
    weather_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "historical_data") \
        .option("driver", "org.sqlite.JDBC") \
        .load()

    # Read air quality data from SQLite
    air_quality_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "air_quality_data") \
        .option("driver", "org.sqlite.JDBC") \
        .load()
    
     # Convert the integer Unix timestamp to a proper timestamp and then to date
    weather_df = weather_df.withColumn("date", to_date(from_unixtime(col("timestamp"))))
    air_quality_df = air_quality_df.withColumn("date", to_date(from_unixtime(col("timestamp"))))

    daily_avg_weather = weather_df.groupBy("date", "city") \
        .agg(avg("temperature").alias("avg_temperature"))

    daily_avg_air_quality = air_quality_df.groupBy("date", "city") \
        .agg(avg("aqi").alias("avg_aqi"))

    # Show the results (for testing)
    daily_avg_weather.show()
    daily_avg_air_quality.show()

    # Optionally, write the results back to SQLite or another output format
    daily_avg_weather.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "daily_avg_weather") \
        .option("driver", "org.sqlite.JDBC") \
        .mode("overwrite") \
        .save()

    daily_avg_air_quality.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "daily_avg_air_quality") \
        .option("driver", "org.sqlite.JDBC") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    process_sqlite_data()
