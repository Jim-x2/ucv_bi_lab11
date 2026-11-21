
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    trim,
    lower,
    when,
    explode
)

# ==========================================================
# 1. CREAR SPARKSESSION CON APACHE ICEBERG
# ==========================================================

spark = (
    SparkSession.builder
    .appName("Products Bronze to Silver Iceberg")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.lakehouse.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3a://ucv-data-platform/warehouse"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# 2. CONFIGURAR CONEXION S3 - FLOCI
# ==========================================================

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://floci:4566")
hadoop_conf.set("fs.s3a.access.key", "test")
hadoop_conf.set("fs.s3a.secret.key", "test")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# ==========================================================
# 3. LEER DATOS BRONZE
# ==========================================================

print("\n========== LEYENDO DATOS BRONZE ==========")

df = (
    spark.read
    .option("multiline", "true")
    .json("s3a://ucv-data-platform/products/bronze/")
)

# ==========================================================
# 4. EXTRAER ARREGLO PRODUCTS
# ==========================================================

df_products = (
    df.select(
        explode(col("products")).alias("product")
    )
    .select("product.*")
)

total_bronze = df_products.count()

print("Productos Bronze:", total_bronze)

# ==========================================================
# 5. TRANSFORMAR Y LIMPIAR DATOS
# ==========================================================

print("\n========== TRANSFORMANDO DATOS ==========")

df_silver = (
    df_products.select(
        col("id"),
        trim(col("title")).alias("title"),
        lower(trim(col("category"))).alias("category"),
        col("price").cast("double").alias("price"),
        col("discountPercentage").cast("double").alias(
            "discount_percentage"
        ),
        col("rating").cast("double").alias("rating"),
        col("stock").cast("integer").alias("stock"),
        col("brand"),
        col("sku"),
        col("availabilityStatus")
    )
    .filter(col("id").isNotNull())
    .filter(col("title").isNotNull())
    .filter(col("price") >= 0)
    .withColumn(
        "brand",
        when(col("brand").isNull(), "unknown")
        .otherwise(trim(col("brand")))
    )
    .withColumn(
        "processed_at",
        current_timestamp()
    )
)

total_silver = df_silver.count()

print("Registros Silver:", total_silver)

df_silver.printSchema()
df_silver.show(5, truncate=False)

# ==========================================================
# 6. CREAR NAMESPACE ICEBERG
# ==========================================================

print("\n========== CREANDO NAMESPACE ICEBERG ==========")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")

print("Namespace silver creado correctamente.")

# ==========================================================
# 7. GUARDAR DATOS EN TABLA ICEBERG
# ==========================================================

print("\n========== GUARDANDO DATOS EN ICEBERG ==========")

(
    df_silver.writeTo("lakehouse.silver.products")
    .using("iceberg")
    .createOrReplace()
)

print("Tabla Iceberg creada correctamente.")

# ==========================================================
# 8. CONSULTAR TABLA ICEBERG
# ==========================================================

print("\n========== CONSULTANDO TABLA ICEBERG ==========")

df_iceberg = spark.table("lakehouse.silver.products")

total_iceberg = df_iceberg.count()

print("Registros almacenados en Iceberg:", total_iceberg)

df_iceberg.printSchema()
df_iceberg.show(5, truncate=False)

# ==========================================================
# 9. VERIFICAR TABLA Y METADATOS
# ==========================================================

print("\n========== TABLAS DEL NAMESPACE SILVER ==========")

spark.sql("""
    SHOW TABLES IN lakehouse.silver
""").show(truncate=False)

print("\n========== HISTORIAL DE ICEBERG ==========")

spark.sql("""
    SELECT *
    FROM lakehouse.silver.products.snapshots
""").show(truncate=False)

# ==========================================================
# 10. FINALIZAR PROCESO
# ==========================================================

print("\n==============================================")
print("TRANSFORMACION BRONZE A SILVER COMPLETADA")
print("TABLA APACHE ICEBERG VERIFICADA")
print("==============================================")

spark.stop()
