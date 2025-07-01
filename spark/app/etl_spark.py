from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, date_format, monotonically_increasing_id

# Configuración de PostgreSQL (¡actualiza estos valores!)
PG_HOST = "postgres"  # Nombre del servicio en compose o IP/host
PG_PORT = "5432"
PG_USER = "usuario"
PG_PASSWORD = "contraseña"
PG_DATABASE = "nombre_db"

# Configuración Spark
spark = SparkSession.builder \
    .appName("ETL Transacciones Financieras") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

# Leer CSV raw
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark/data/transactions.csv")

# Dimensión Fecha
df_fecha = df_raw \
    .withColumn("fecha", to_date("timestamp")) \
    .withColumn("anio", year(col("fecha"))) \
    .withColumn("mes", month(col("fecha"))) \
    .withColumn("dia", dayofmonth(col("fecha"))) \
    .withColumn("dia_semana", date_format(col("fecha"), "EEEE")) \
    .select("fecha", "anio", "mes", "dia", "dia_semana") \
    .distinct()

# Dimensión Tarjeta
df_tarjeta = df_raw \
    .select(
        col("service_types").alias("tipo_tarjeta"),
        col("brand").alias("red"),
        col("institution_id").alias("banco")
    ) \
    .distinct() \
    .withColumn("tarjeta_id", monotonically_increasing_id())

# Dimensión Usuario
df_usuario = df_raw \
    .select(col("user_id").alias("nombre_usuario")) \
    .distinct() \
    .withColumn("usuario_id", monotonically_increasing_id())

# Dimensión Ubicación
df_ubicacion = df_raw \
    .select("lat", "lon") \
    .distinct() \
    .withColumn("ubicacion_id", monotonically_increasing_id())

# Dimensión Transacción
df_transaccion = df_raw \
    .select(col("transaction_type").alias("descripcion")) \
    .distinct() \
    .withColumn("transaccion_tipo_id", monotonically_increasing_id())

# Dimensión Moneda
df_moneda = df_raw \
    .select(col("currency").alias("codigo_moneda")) \
    .distinct() \
    .withColumn("moneda_id", monotonically_increasing_id())

# Tabla de hechos (sin comercio porque no existe en tu CSV)
df_fact = df_raw \
    .withColumn("fecha", to_date("timestamp")) \
    .join(df_fecha, on="fecha", how="left") \
    .join(df_tarjeta,
          (df_raw["service_types"] == df_tarjeta["tipo_tarjeta"]) &
          (df_raw["brand"] == df_tarjeta["red"]) &
          (df_raw["institution_id"] == df_tarjeta["banco"]),
          "left") \
    .join(df_usuario,
          df_raw["user_id"] == df_usuario["nombre_usuario"],
          "left") \
    .join(df_ubicacion,
          on=["lat", "lon"],
          how="left") \
    .join(df_transaccion,
          df_raw["transaction_type"] == df_transaccion["descripcion"],
          "left") \
    .join(df_moneda,
          df_raw["currency"] == df_moneda["codigo_moneda"],
          "left")

df_fact_final = df_fact.select(
    col("transaction_id"),
    col("fecha"),
    col("anio"),
    col("mes"),
    col("dia"),
    col("dia_semana"),
    col("tarjeta_id"),
    col("usuario_id"),
    col("ubicacion_id"),
    col("transaccion_tipo_id"),
    col("moneda_id"),
    col("amount").alias("monto"),
    col("timestamp")
)

# Función para escribir en PostgreSQL
def write_to_postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}") \
        .option("dbtable", table_name) \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .mode("overwrite") \
        .save()

# Guardar dimensiones y hechos
write_to_postgres(df_fecha, "dim_fecha")
write_to_postgres(df_tarjeta, "dim_tarjeta")
write_to_postgres(df_usuario, "dim_usuario")
write_to_postgres(df_ubicacion, "dim_ubicacion")
write_to_postgres(df_transaccion, "dim_transaccion")
write_to_postgres(df_moneda, "dim_moneda")
write_to_postgres(df_fact_final, "fact_transacciones")

print("✅ Datos escritos en PostgreSQL exitosamente!")

spark.stop()
