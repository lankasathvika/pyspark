from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# Create Spark session
spark = SparkSession.builder \
    .appName("PySpark Clustering Example") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Starting the PySpark Clustering script...")

# Sample Data
data = [
    (1, 1.0, 1.0),
    (2, 1.1, 1.1),
    (3, 0.9, 0.8),
    (4, 5.0, 5.0),
    (5, 6.0, 5.5),
    (6, 5.5, 6.0)
]

df = spark.createDataFrame(data, ["id", "feature1", "feature2"])

# Combine features into vector
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
df_vector = assembler.transform(df)

# Train KMeans model
kmeans = KMeans(k=2, seed=1)
model = kmeans.fit(df_vector)

# Make predictions
predictions = model.transform(df_vector)

# Display predictions
predictions.select("id", "features", "prediction").show()

# Show cluster centers
print("Cluster Centers:")
for center in model.clusterCenters():
    print(center)

# Stop Spark session
spark.stop()
