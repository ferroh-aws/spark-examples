package org.example.spark

import org.apache.iceberg.spark.IcebergSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object IngestCustomersIceberg {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("No location provided for customers file.")
      System.exit(-1)
    } else {
      val csv = args(0)
      val spark = SparkSession
        .builder()
        .getOrCreate()
      IcebergSpark.registerBucketUDF(spark, "iceberg_buckets", DataTypes.StringType, 20)
      val schema = StructType(
        Seq(
          StructField("id", StringType, false),
          StructField("first_name", StringType, false),
          StructField("last_name", StringType, false),
          StructField("zip", StringType, false),
          StructField("street", StringType, false),
          StructField("city", StringType, false),
          StructField("card", StringType, false),
          StructField("updated", TimestampType, false)
        )
      )
      spark
        .read
        .option("header", "true")
        .schema(schema)
        .csv(csv)
        .createOrReplaceTempView("temp_customers")
      spark.sql(
        """MERGE INTO glue.icebergdb.customers c
          |USING (SELECT * FROM temp_customers ORDER BY iceberg_buckets(id)) tc
          |ON c.id = tc.id
          |WHEN MATCHED THEN UPDATE SET c.updated = tc.updated
          |WHEN NOT MATCHED THEN INSERT *
          |""".stripMargin)
      spark.stop()
    }
  }
}
