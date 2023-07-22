# Ejemplos de Apache Spark

En estos ejemplos utilizamos:

- Amazon Elastic Map Reduce [(EMR)](https://aws.amazon.com/emr/) versión 6.11.0
- Amazon Managed Streaming for Kafka [(MSK)](https://aws.amazon.com/msk/?nc2=h_ql_prod_an_msak) versión 2.8.1
- [AWS Lake Formation](https://aws.amazon.com/lake-formation/?nc2=h_ql_prod_an_lkf)
- [Apache Iceberg](https://iceberg.apache.org/) versión 1.2.0

## Notas de configuración

Para poder utilizar Amazon EMR integrado con AWS Lake Formation utilizando Apache Iceberg como formato de lago de datos transaccional es necesario realizar las siguientes configuraciones:

### Crear configuración de seguridad

Ejemplo de configuración de seguridad:

```json
{
    "AuthorizationConfiguration":{
        "IAMConfiguration":{
            "EnableApplicationScopedIAMRole":true,
            "ApplicationScopedIAMRoleConfiguration":{
                "PropagateSourceIdentity":true
            }
        },
        "LakeFormationConfiguration":{
            "AuthorizedSessionTagValue":"Amazon EMR"
        }
    }
}
```
Es posible reforzar la encripción utilizando esta configuración, para mayor detalle [aquí](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-create-security-configuration.html).

### Configuración de aplicaciones

Para utilizar Apache Iceberg y AWS Lake Formation es necesario activar ambas funciones en la configuración, a continuación se muestra el JSON de ejemplo.

```json
[
    {
        "Classification": "iceberg-defaults",
        "Properties": {
            "iceberg.enabled": "true"
        }
    },
    {
        "Classification": "hive-site",
        "Properties": {
            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        }
    },
    {
        "Classification": "spark-hive-site",
        "Properties": {
            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        }
    }
]
```

### Configuración para Streaming

Con la finalidad de evitar descargar los Jars cada que se ejecuta un step en EMR, se recomienda realizar una acción de bootstrap. En este ejemplo se utiliza un bucket de Amazon Simple Storage Service, lo recomedable es generar una imagen personalizada (Custom AMI) y copiar en ella los Jars para posteriormente copiarlos a la ruta final con una acción de arranque (bootstrap action):

Ejemplo de script para bootstrap desde un bucket donde los Jars se colocan con el prefijo ```jars/```:

```bash
#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
fi

BUCKET_NAME=$1

sudo aws s3 cp s3://$BUCKET_NAME/jars /usr/lib/spark/jars/ --recursive --exclude "*" --include "*.jar"
```
Los Jars a continuación son necesarios para poder realizar la conexión a Amazon MSK utilizando autenticación por AWS IAM y prevenir un error al utilizar Amazon DynamoDB como tabla de bloqueo para Apache Iceberg. El Jar bundle-version.jar es el AWS SDK para Java versión 2 en su totalidad.

```
aws-msk-iam-auth-1.1.7-all.jar
bundle-2.20.108.jar
commons-pool2-2.11.0.jar
kafka-clients-2.8.0.jar
spark-sql-kafka-0-10_2.12-3.3.2.jar
spark-streaming_2.12-3.3.2.jar
spark-tags_2.12-3.3.2.jar
spark-token-provider-kafka-0-10_2.12-3.3.2.jar
```

**NOTA** Los Jars para spark listados previamente son compatibles con la versión de EMR 6.11.0, utilizan Scala 2.12 y Spark 3.3.2, para mayor detalles en las versiones incluidas en EMR visitar [EMR Release 6.11.0](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-6110-release.html).

Ejemplos:

- [Ingesta Spark](SPARK.md)
- [Structured Streaming](STREAMING.md)
- [Mantenimiento](MAINTENANCE.md)