from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("create-table").getOrCreate()

schema = StructType(
    [
        StructField("Name", StringType(), True),
        StructField("Capital", StringType(), True),
        StructField("Area", IntegerType(), True),
        StructField("Population", IntegerType(), True),
    ]
)

df = spark.createDataFrame(
    [
        ("Австралия", "Канберра", 7686850, 19731984),
        ("Австрия", "Вена", 83855, 7700000),
    ],
    schema,
)

df.write.mode("overwrite").option("header", "true").csv(
    "s3a://etl-postgresql-storage-kate/countries"
)

spark.stop()
