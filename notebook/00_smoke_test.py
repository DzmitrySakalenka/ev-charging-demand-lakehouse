from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.range(10).write.format("delta").mode("overwrite").saveAsTable(
    "ev_charging_demo.bronze.smoke_test",
)
