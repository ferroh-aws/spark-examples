package org.example.spark

import org.apache.iceberg.spark.IcebergSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types._

object IngestIceberg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    IcebergSpark.registerBucketUDF(spark, "iceberg_buckets", DataTypes.StringType, 20)
    val customers = spark
      .read
      .option("header", "true")
      .csv("s3://data.aws-poc-hf.com/raw-data/card-gen-customers.csv")
      .sortWithinPartitions(expr("iceberg_buckets(id)"))
    customers
      .createOrReplaceTempView("temp_customers")
    spark.sql(
      """MERGE INTO glue.icebergdb.customers c
        |USING (SELECT * FROM temp_customers) tc
        |ON c.id = tc.id
        |WHEN NOT MATCHED THEN INSERT *
        |""".stripMargin)
    val transactions = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("s3://data.aws-poc-hf.com/raw-data/card-gen-txs.csv")
      .withColumnRenamed("timestamp", "op_ts")
    transactions.createOrReplaceTempView("temp_transactions")
    spark.sql(
      """MERGE INTO glue.icebergdb.transactions t
        |USING (SELECT * FROM temp_transactions ORDER BY iceberg_buckets(id), op_ts) tt
        |ON t.id = tt.id
        |WHEN NOT MATCHED THEN INSERT *
        |""".stripMargin)
    spark.stop()
  }
}
