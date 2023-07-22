# Ejemplo de Ingesta


## Tabla
Para este ejemplo utilizamos la siguiente estructura de tabla:

```sql
CREATE TABLE IF NOT EXISTS glue.icebergdb.customers (
    id string,
    first_name string,
    last_name string,
    updated timestamp,
    zip string,
    street string,
    city string,
    card string
) USING iceberg
PARTITIONED BY (bucket(20, id))
location 's3://data.aws-poc-hf.com/iceberg/db/customers'
```
Configuraciones:

- PARTITIONED: Apache Iceberg utiliza particionamiento oculto, lo que nos permite aplicar transformaciones en la columna de partición como es bucket, para mayor información referirse a la documentación de [Apache Iceberg](https://iceberg.apache.org/docs/1.2.0/partitioning/).
- LOCATION: Ruta de Amazon S3 donde se persistirán los datos. Esta ruta debería estar registrada en las localidades de datos dentro de AWS LakeFormation.

## Configuración de Step

Para ejecutar el proceso de spark lanzaremos un step en Amazon EMR, a continuación se muestra un ejemplo:

```json
[
  {
    "Type": "Spark",
    "Name": "IngestToIceberg",
    "ActionOnFailure": "CONTINUE",
    "Args": [
      "--master", "yarn",
      "--deploy-mode", "cluster",
      "--executor-memory", "2g",
      "--num-executors", "6",
      "--executor-cores", "1",
      "--class", "org.example.spark.IngestCustomersIceberg",
      "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "--conf", "spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog",
      "--conf", "spark.sql.catalog.glue.warehouse=s3://data.aws-poc-hf.com/iceberg-warehouse",
      "--conf", "spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "--conf", "spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
      "--conf", "spark.sql.catalog.glue.lock-impl=org.apache.iceberg.aws.dynamodb.DynamoDbLockManager",
      "--conf", "spark.sql.catalog.glue.lock.table=IcebergLockTable",
      "s3://data.aws-poc-hf.com/spark-jobs/spark-examples_2.13-0.1.0-SNAPSHOT.jar",
      "s3://data.aws-poc-hf.com/raw-data/test-data-customers.csv"
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

## Proceso Spark

Una limitación de Apache Iceberg 1.2.0 es la necesidad de ordenar la información por particiones previamente a la escritura, con la mayoría de las estrategias de particionamiento basta con agregar un ```ORDER BY col```, en el caso de particiones con buckets es necesario registrar una User Defined Function (UDF) para realizar el ordenamiento, ej:

```scala
import org.apache.iceberg.spark.IcebergSpark

IcebergSpark.registerBucketUDF(spark, "iceberg_buckets", DataTypes.StringType, 20)
```

Donde:
- spark es la sesión de SparkSQL.
- iceberg_buckets es el nombre con el cual invocaremos la función.
- DataTypes.StringType es el tipo de dato de la columna sobre la que se realiza el particionamiento.
- 20 es la cantidad de buckets definidos durante la creación de la tabla.

Ejemplo de SQL con MERGE y uso de UDF:

```sql
MERGE INTO glue.icebergdb.customers c
USING (SELECT * FROM temp_customers ORDER BY iceberg_buckets(id)) tc
ON c.id = tc.id
WHEN MATCHED THEN UPDATE SET c.updated = tc.updated, c.card = tc.card
WHEN NOT MATCHED THEN INSERT *
```