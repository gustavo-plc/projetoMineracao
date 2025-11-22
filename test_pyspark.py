import sys
import os
from pyspark.sql import SparkSession

# --- ADICIONE ESTAS DUAS LINHAS ---
# Elas forçam o Spark a usar o mesmo Python que está rodando o script (o do venv)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# ----------------------------------

spark = SparkSession.builder.appName("Test").getOrCreate()

df = spark.createDataFrame([(1, "Gustavo"), (2, "Teste")], ["id", "nome"])
df.show()

spark.stop()