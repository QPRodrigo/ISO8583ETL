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
    
# Dimensión de Codigo de Respuesta
df_respCode = df_raw \
    .select(col("response_code").alias("codigo_respuesta")) \
    .distinct() \
    .withColumn("codigo_respuesta_id", monotonically_increasing_id())
    
# Dimensión Tipo de tarjeta  
df_typeService = df_raw \
    .select(col("service_types").alias("tipo_tarjeta")) \
    .distinct() \
    .withColumn("tipo_tarjeta_id", monotonically_increasing_id())

# Dimensión Banco
df_bank = df_raw \
    .select(col("institution_id").alias("banco")) \
    .distinct() \
    .withColumn("banco_id", monotonically_increasing_id())   
    
# Dimensión Marca Tarjeta
df_brand = df_raw \
    .select(col("brand").alias("marca_tarjeta")) \
    .distinct() \
    .withColumn("marca_tarjeta_id", monotonically_increasing_id())   
    
# Dimensión Transacción
df_transaccion = df_raw \
    .select(col("transaction_type").alias("tipo_transaccion")) \
    .distinct() \
    .withColumn("tipo_transaccion_id", monotonically_increasing_id())
        
# Dimensión Usuario
df_usuario = df_raw \
    .select(col("user_id").alias("nombre_usuario")) \
    .distinct() \
    .withColumn("usuario_id", monotonically_increasing_id())
    
# Dimensión Moneda
df_moneda = df_raw \
    .select(col("currency").alias("codigo_moneda")) \
    .distinct() \
    .withColumn("moneda_id", monotonically_increasing_id())
    
# Dimensión Ubicación
df_ubicacion = df_raw \
    .select("lat", "lon") \
    .distinct() \
    .withColumn("ubicacion_id", monotonically_increasing_id())


# Tabla de hechos
df_fact = df_raw \
    .withColumn("fecha", to_date("timestamp")) \
    .join(df_fecha, on="fecha", how="left") \
    .join(df_respCode,
          df_raw["response_code"] == df_respCode["codigo_respuesta"],
          "left") \
    .join(df_typeService,
          df_raw["service_types"] == df_typeService["tipo_tarjeta"],
          "left") \
    .join(df_bank,
          df_raw["institution_id"] == df_bank["banco"],
          "left") \
    .join(df_brand,
          df_raw["brand"] == df_brand["marca_tarjeta"],
          "left") \
    .join(df_transaccion,
          df_raw["transaction_type"] == df_transaccion["tipo_transaccion"],
          "left") \
    .join(df_usuario,
          df_raw["user_id"] == df_usuario["nombre_usuario"],
          "left") \
    .join(df_moneda,
          df_raw["currency"] == df_moneda["codigo_moneda"],
          "left") \
    .join(df_ubicacion,
            on=["lat", "lon"],
            how="left")
    
df_fact_final = df_fact.select(
    col("transaction_id"),
    col("fecha"),
    col("anio"),
    col("mes"),
    col("dia"),
    col("dia_semana"),
    col("codigo_respuesta_id"),  
    col("tipo_tarjeta_id"), 
    col("banco_id"), 
    col("marca_tarjeta_id"), 
    col("tipo_transaccion_id"), 
    col("usuario_id"), 
    col("moneda_id"), 
    col("ubicacion_id"),
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
write_to_postgres(df_respCode, "dim_respCode")
write_to_postgres(df_typeService, "dim_tipoServicio")
write_to_postgres(df_bank, "dim_banco")
write_to_postgres(df_brand, "dim_marca")
write_to_postgres(df_transaccion, "dim_transaccion")
write_to_postgres(df_usuario, "dim_usuario")
write_to_postgres(df_moneda, "dim_moneda")
write_to_postgres(df_ubicacion, "dim_ubicacion")
write_to_postgres(df_fact_final, "fact_transacciones")

print("✅ Datos escritos en PostgreSQL exitosamente!")

spark.stop()