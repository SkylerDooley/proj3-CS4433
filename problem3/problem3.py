"""
Bonus Problem 3:
Predict TransTotal using Spark MLlib Regression
Features: Age, Salary, TransNumItems
Target:   TransTotal
"""

import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\tools\\hadoop-3.2.2"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Start Spark
spark = SparkSession.builder \
    .appName("Bonus_P3_MLlib") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load datasets
customers = spark.read.csv("../datasets/Customers.csv", header=True, inferSchema=True)
purchases = spark.read.csv("../datasets/Purchases.csv", header=True, inferSchema=True)

# Join on CustID to get Age + Salary + purchase info
data = purchases.join(customers, on="CustID")

# Select only the columns needed for ML
# Features: Age, Salary, TransNumItems
# Label:    TransTotal
ml_data = data.select(
    col("Age"),
    col("Salary"),
    col("TransNumItems"),
    col("TransTotal").alias("label")
)

# Assemble features into a single vector
assembler = VectorAssembler(
    inputCols=["Age", "Salary", "TransNumItems"],
    outputCol="features"
)

final_data = assembler.transform(ml_data).select("features", "label")

# Train/Test split (80% / 20%)
train, test = final_data.randomSplit([0.8, 0.2], seed=42)

print(f"Training rows: {train.count():,}")
print(f"Testing rows : {test.count():,}")

# Train a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label")
model = lr.fit(train)

print("\nModel trained.")
print(f"Coefficients: {model.coefficients}")
print(f"Intercept:    {model.intercept}")

# Apply model to test set
predictions = model.transform(test)
predictions.show(10, truncate=False)

# Evaluate model performance
evaluator_rmse = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="rmse"
)

evaluator_mae = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="mae"
)

rmse = evaluator_rmse.evaluate(predictions)
mae  = evaluator_mae.evaluate(predictions)

print("\nEvaluation Metrics:")
print(f"  RMSE: {rmse:.4f}")
print(f"  MAE : {mae:.4f}")

spark.stop()
