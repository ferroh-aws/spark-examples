# Ejemplo de mantenimiento

## Ejemplo de Step

```json
[
  {
    "Type": "Spark",
    "Name": "Table Maintenance",
    "ActionOnFailure": "CONTINUE",
    "Args": [
      "--master", "yarn",
      "--deploy-mode", "cluster",
      "--executor-memory", "2g",
      "--num-executors", "1",
      "--executor-cores", "1",
      "--class", "org.example.spark.streaming.TableMaintenance",
      "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "--conf", "spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog",
      "--conf", "spark.sql.catalog.glue.warehouse=s3://data.aws-poc-hf.com/iceberg-warehouse",
      "--conf", "spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "--conf", "spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
      "--conf", "spark.sql.catalog.glue.lock-impl=org.apache.iceberg.aws.dynamodb.DynamoDbLockManager",
      "--conf", "spark.sql.catalog.glue.lock.table=IcebergLockTable",
      "s3://data.aws-poc-hf.com/spark-jobs/spark-examples_2.13-0.1.0-SNAPSHOT.jar",
      "glue",
      "icebergdb",
      "streaming_txs",
      "10",
      "500"
    ]
  }
]
```
El nombre de catálogo es personalizable, en este ejemplo usaremos ```glue```.

Configuraciones:
- spark.sql.extensions - Esta configuración registra las extensiones de SQL de Apache Iceberg en la sesión de Spark.
- spark.sql.catalog.glue - Esta configuración establece el tipo de catálogo a utilizar en la sesión de Spark.
- spark.sql.catalog.glue.warehouse - Ruta al warehouse de Iceberg.
- spark.sql.catalog.glue.catalog-impl - Implementación del catálogo de Iceberg, en este caso utilizamos el de Glue.
- spark.sql.catalog.glue.io-impl - Implementación de I/O de Iceberg, utilizamos S3.
- spark.sql.catalog.glue.lock-impl - Implementación de estrategia de bloqueo, en este caso usamos Amazon DynamoDB.
- spark.sql.catalog.glue.lock.table - Nombre de la tabla en Amazon DynamoDB.

# Proceso Spark

Para realizar el mantenimiento de las tablas es posible utilizar los procedimientos almacenados de [Apache Iceberg](https://iceberg.apache.org/docs/1.2.0/spark-procedures/).

```scala
  spark.sql(
    s"""
       |CALL $catalogName.system.expire_snapshots('$databaseName.$tableName', TIMESTAMP '${expire.toString}')
       |""".stripMargin)
  spark.sql(
    s"""
       |CALL $catalogName.system.rewrite_data_files('$databaseName.$tableName')
       |""".stripMargin)
  spark.sql(
    s"""
       |CALL $catalogName.system.rewrite_manifests('$databaseName.$tableName')
       |""".stripMargin)
  spark.sql(
    s"""
       |CALL $catalogName.system.remove_orphan_files('$databaseName.$tableName')
       |""".stripMargin)
```

Acciones:
- expire_snapshots - Elimina los snapshots utilizados por time travel.
- rewrite_data_files - Optimiza los archivos de acorde a la definición de la tabla.
- rewrite_manifests - Optimiza los archivos manifest los cuales son utilizados para optimizar el scan planning.
- remove_orphan_files - Elimina los archivos que puedan no tener referencia alguna. Esta tarea debe ser utilizada de forma cautelosa con archivos relativamente nuevos, lo recomendable es borrar archivos con más de tres días de antigüedad.