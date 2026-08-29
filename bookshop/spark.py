"""A SparkSession wired to the REST catalog (chapter 10 onward).

The jar coordinate encodes THREE versions -- the Spark minor, the Scala
minor, and Iceberg -- and all three must line up with the installed
PySpark. `pip install pyspark` currently gives 4.2.0, for which no Iceberg
runtime exists; the pin in pyproject.toml is deliberate.
"""
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def spark_session(app: str = "bookshop", uri: str = None, memory: str = "2g"):
    from pyspark.sql import SparkSession
    from bookshop.catalog import REST_URI

    s = (
        SparkSession.builder.appName(app)
        .config("spark.jars.packages", ICEBERG_RUNTIME)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "rest")
        .config("spark.sql.catalog.ice.uri", uri or REST_URI)
        .config("spark.sql.defaultCatalog", "ice")
        .config("spark.driver.memory", memory)
        .master("local[2]")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    return s
