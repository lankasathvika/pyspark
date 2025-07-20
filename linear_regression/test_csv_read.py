from pyspark.sql import SparkSession

# Step 1: Start Spark session
spark = SparkSession.builder.appName("TestCSV").getOrCreate()

# Step 2: Read the CSV file
data = spark.read.csv("data/consulting_data.csv", header=True, inferSchema=True)

# Step 3: Print schema and data
print("=== SCHEMA ===")
data.printSchema()

print("=== DATA ===")
data.show()

# Step 4: Stop Spark
spark.stop()
