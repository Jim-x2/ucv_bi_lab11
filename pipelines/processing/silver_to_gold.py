

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    max,
    min,
    sum,
    round,
    current_timestamp
)

# 1. Crear SparkSession con Apache Iceberg
spark = (
    SparkSession.builder
    .appName("Products Silver to Gold Iceberg")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.warehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.warehouse.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.warehouse.warehouse",
        "s3a://ucv-data-platform/warehouse"
    )
    .getOrCreate()
)

# 2. Configuracion S3 - Floci
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://floci:4566")
hadoop_conf.set("fs.s3a.access.key", "test")
hadoop_conf.set("fs.s3a.secret.key", "test")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# 3. Crear namespace Gold
spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS warehouse.gold
""")

# 4. Leer datos Silver desde Iceberg
print("Leyendo datos Silver desde Iceberg...")

df = spark.read.format("iceberg").load(
    "warehouse.silver.products"
)

print("Registros Silver:", df.count())

# 5. Calcular indicadores Gold
print("Calculando indicadores Gold...")

df_gold = (
    df.groupBy("category")
    .agg(
        count("id").alias("total_products"),
        round(avg("price"), 2).alias("average_price"),
        max("price").alias("maximum_price"),
        min("price").alias("minimum_price"),
        sum("stock").alias("total_stock"),
        round(avg("rating"), 2).alias("average_rating")
    )
    .withColumn("processed_at", current_timestamp())
)

# 6. Mostrar resultados
print("Resultados Gold:")

print("Esquema Gold:")
df_gold.printSchema()

df_gold.show(truncate=False)

print("Total de categorias:", df_gold.count())

# 7. Guardar Gold como tabla Apache Iceberg
print("Guardando datos Gold en Iceberg...")

(
    df_gold.writeTo("warehouse.gold.products")
    .using("iceberg")
    .createOrReplace()
)

# 8. Verificar tabla Gold
print("Verificando tabla Gold...")

df_gold_verification = spark.read.format("iceberg").load(
    "warehouse.gold.products"
)

df_gold_verification.show(truncate=False)

print("Registros Gold:", df_gold_verification.count())

print("========================================")
print("TRANSFORMACION SILVER A GOLD COMPLETADA")
print("TABLA APACHE ICEBERG VERIFICADA")
print("========================================")

spark.stop()
