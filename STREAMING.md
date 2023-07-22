# Ejemplo de Structured Streaming

# Tabla

```sql
CREATE TABLE IF NOT EXISTS glue.icebergdb.streaming_txs (
    id string,
    updated_on timestamp,
    city string,
    zip string,
    commerce string,
    amount decimal(10,2),
    status string
) USING iceberg
PARTITIONED BY (bucket(20, id))
location 's3://data.aws-poc-hf.com/iceberg/db/streaming_txs'
```
Configuraciones:

- PARTITIONED: Apache Iceberg utiliza particionamiento oculto, lo que nos permite aplicar transformaciones en la columna de partición como es bucket, para mayor información referirse a la documentación de [Apache Iceberg](https://iceberg.apache.org/docs/1.2.0/partitioning/).
- LOCATION: Ruta de Amazon S3 donde se persistirán los datos. Esta ruta debería estar registrada en las localidades de datos dentro de AWS LakeFormation.

## Configuración de Step

Ejemplo:

```json
[
  {
    "Type": "Spark",
    "Name": "IngestToIceberg",
    "ActionOnFailure": "CONTINUE",
    "Args": [
      "--master", "yarn",
      "--deploy-mode", "cluster",
      "--executor-memory", "4g",
      "--num-executors", "2",
      "--executor-cores", "2",
      "--class", "org.example.spark.streaming.TransactionStreaming",
      "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "--conf", "spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog",
      "--conf", "spark.sql.catalog.glue.warehouse=s3://data.aws-poc-hf.com/iceberg-warehouse",
      "--conf", "spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "--conf", "spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
      "--conf", "spark.sql.catalog.glue.lock-impl=org.apache.iceberg.aws.dynamodb.DynamoDbLockManager",
      "--conf", "spark.sql.catalog.glue.lock.table=IcebergLockTable",
      "--conf", "spark.sql.iceberg.handle-timestamp-without-timezone=true",
      "s3://data.aws-poc-hf.com/spark-jobs/spark-examples_2.13-0.1.0-SNAPSHOT.jar",
      "b-3.streaming.eksxgm.c8.kafka.us-east-1.amazonaws.com:9098,b-2.streaming.eksxgm.c8.kafka.us-east-1.amazonaws.com:9098,b-1.streaming.eksxgm.c8.kafka.us-east-1.amazonaws.com:9098",
      "tx_topic",
      "s3://data.aws-poc-hf.com/checkpoint/streaming_txs"
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
- spark.sql.iceberg.handle-timestamp-without-timezone - Spark no es capaz de procesar timestamps sin zona horaria, con esta bandera Iceberg trata los timestamps sin zona horaria.

## Proceso Spark

```scala
import org.apache.iceberg.spark.IcebergSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.util.concurrent.TimeUnit

val stream = df.writeStream
  .format("iceberg")
  .outputMode("append")
  .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
  .option("path", "glue.icebergdb.streaming_txs")
  .option("fanout-enabled", "true")
  .option("checkpointLocation", args(2))
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    batchDF.persist()
    batchDF.createOrReplaceTempView("incoming_txs")
    batchDF.sparkSession.sql(
      """MERGE INTO glue.icebergdb.streaming_txs t
        |USING (SELECT * FROM incoming_txs) it
        |ON t.id = it.id
        |WHEN MATCHED THEN UPDATE SET t.updated_on = it.updated_on, t.status = it.status
        |WHEN NOT MATCHED THEN INSERT *
        |""".stripMargin
    )
    batchDF.unpersist()
    ()
  }
```
Opciones de configuración:
- format - Formato de escritura, ```iceberg``` en este ejemplo.
- outputMode - ```append``` agregar registros al final de la tabla o ```complete``` el cuál reescribe toda la tabla.
- option path - Tabla sobre la que realizaremos la escritura.
- option fanout-enabled - Para evitar tener que ordenar los datos por partición podemos activar esta función.
- option checkpointLocation - Ruta donde se mantiene el estado de la transacción de streaming, se recomienda utilizar una ruta de S3.
