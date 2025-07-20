from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("PySpark is working!")

# Create dummy data
data = [("A", 10), ("B", 20)]
df = spark.createDataFrame(data, ["name", "value"])
df.show()

spark.stop()
