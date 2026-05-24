import sys

from pyspark.sql import SparkSession

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("process-x-tab").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

result = df.orderBy("id")

result.write.mode("overwrite").csv(output_path, header=True)

spark.stop()
