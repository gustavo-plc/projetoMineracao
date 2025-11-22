from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

df = spark.createDataFrame([(1, "Gustavo"), (2, "Teste")], ["id", "nome"])
df.show()

spark.stop()
