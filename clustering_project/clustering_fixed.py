from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

#  Create Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("PySpark Clustering") \
    .getOrCreate()

#  Reduce logs
spark.sparkContext.setLogLevel("ERROR")

print(" Spark session started...")

#  Sample clustering data
data = [
    (1, 1.0, 1.0),
    (2, 1.1, 1.1),
    (3, 0.9, 0.8),
    (4, 5.0, 5.0),
    (5, 6.0, 5.5),
    (6, 5.5, 6.0)
]

#  Create DataFrame
df = spark.createDataFrame(data, ["id", "feature1", "feature2"])
print(" Sample DataFrame created:")
df.show()

#  Convert features to vector
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
df_vector = assembler.transform(df)

#  Train KMeans
kmeans = KMeans(k=2, seed=1)
model = kmeans.fit(df_vector)

#  Predict
predictions = model.transform(df_vector)
print(" Predictions:")
predictions.select("id", "features", "prediction").show()

#  Show cluster centers
print(" Cluster Centers:")
for center in model.clusterCenters():
    print(center)

# Close session
spark.stop()
print(" Spark session stopped.")
