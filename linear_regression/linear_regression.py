from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Create Spark session
spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

# Load dataset
data = spark.read.csv("data/consulting_data.csv", header=True, inferSchema=True)
print("=== Loaded Data ===")
data.printSchema()
data.show()

# Create feature vector column
assembler = VectorAssembler(
    inputCols=["feature1", "feature2"],
    outputCol="features"
)
final_data = assembler.transform(data).select("features", "target_column")

#Split data into training and test sets
train_data, test_data = final_data.randomSplit([0.8, 0.2])

# Train Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="target_column")
lr_model = lr.fit(train_data)

# Print model coefficients and intercept
print("=== Model Info ===")
print(f"Coefficients: {lr_model.coefficients}")
print(f"Intercept: {lr_model.intercept}")

#  Make predictions on test data
predictions = lr_model.transform(test_data)

#  Show predictions
print("=== Predictions ===")
predictions.select("features", "target_column", "prediction").show()

# Stop Spark session
spark.stop()
