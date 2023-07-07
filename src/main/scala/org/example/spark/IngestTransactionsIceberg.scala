package org.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object IngestTransactionsIceberg {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Missing csv path")
      System.exit(-1)
    } else {
      val csv = args(0)
      val spark = SparkSession
        .builder()
        .getOrCreate()
      val schema = StructType(
        Seq(
          StructField("id", StringType, false),
          StructField("timestamp", TimestampType, false),
          StructField("city", StringType, false),
          StructField("zip", StringType, false),
          StructField("commerce", StringType, false),
          StructField("amount", StringType, false),
          StructField("status", StringType, false)
        )
      )
      spark
        .read
        .option("header", "true")
        .schema(schema)
        .csv(csv)
        .withColumnRenamed("timestamp", "op_ts")
        .createOrReplaceTempView("temp_transactions")
      spark.sql(
        """MERGE INTO glue.icebergdb.transactions t
          |USING (SELECT * FROM temp_transactions ORDER BY op_ts) tt
          |ON t.id = tt.id
          |WHEN NOT MATCHED THEN INSERT *
          |""".stripMargin)
      spark.stop()
    }
  }
}
