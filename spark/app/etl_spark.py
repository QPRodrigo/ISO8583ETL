from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Configuración de PostgreSQL (¡actualiza estos valores!)
PG_HOST = "postgres"  # Nombre del servicio en compose o IP/host
PG_PORT = "5432"
PG_USER = "usuario"
PG_PASSWORD = "contraseña"
PG_DATABASE = "nombre_db"
PG_TABLE = "transacciones"  # Tabla destino

# Crear SparkSession (el driver ya está en classpath)
spark = SparkSession.builder \
    .appName("ETL-PostgreSQL") \
    .getOrCreate()  # ¡No necesitas configurar spark.jars manualmente!

# Leer CSV
df = spark.read.csv("/opt/spark/data/transactions.csv", header=True, inferSchema=True)
df = df.withColumn("timestamp", to_timestamp("timestamp"))  # Conversión opcional

# Escribir en PostgreSQL (¡modo mejorado!)
(df.write
    .format("jdbc")
    .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    .option("dbtable", PG_TABLE)
    .option("user", PG_USER)
    .option("password", PG_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", 10000)  # Mejora rendimiento
    .mode("append")  # O "overwrite" según necesites
    .save())

print("✅ Datos escritos en PostgreSQL exitosamente!")
spark.stop()