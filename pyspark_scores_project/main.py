try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, avg

    print(" Script started")

    # Step 1: Create SparkSession
    print(" Creating Spark session...")
    spark = SparkSession.builder \
        .appName("Student Score Analysis") \
        .master("local[*]") \
        .getOrCreate()
    print(" Spark session created")

    # Step 2: Load CSV
    print(" Loading students.csv...")
    df = spark.read.csv("students.csv", header=True, inferSchema=True)
    print(" CSV loaded successfully")
    df.show()

    # Step 3: Average per subject
    print(" Average Score per Subject:")
    df.select(
        avg("math").alias("avg_math"),
        avg("english").alias("avg_english"),
        avg("science").alias("avg_science")
    ).show()

    # Step 4: Add total column and top 3 students
    df_with_total = df.withColumn("total", col("math") + col("english") + col("science"))
    top_students = df_with_total.orderBy(col("total").desc()).limit(3)
    print("🏆 Top 3 Students:")
    top_students.select("name", "total").show()

    # Step 5: Add Pass/Fail column (pass if total >= 180)
    from pyspark.sql.functions import col, when

    result_df = df_with_total.withColumn(
    "result",
    when(col("total") >= 180, "Pass").otherwise("Fail")
)
    print("🎓 Final Result with Pass/Fail:")
    result_df.select("name", "total", "result").show()

    # Step 6: Stop Spark session
    spark.stop()
    print(" Spark session stopped")

except Exception as e:
    print(" An error occurred:", e)
