package org.example.spark.streaming

import org.apache.iceberg.spark.IcebergSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.util.concurrent.TimeUnit

object TransactionStreaming {
  private val schema = StructType(
    Seq(
      StructField("id", StringType, nullable = false),
      StructField("updatedOn", TimestampType, nullable = false),
      StructField("city", StringType, nullable = false),
      StructField("zip", StringType, nullable = false),
      StructField("commerce", StringType, nullable = false),
      StructField("amount", DecimalType(10, 2), nullable = false),
      StructField("status", StringType, nullable = false)
    )
  )
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    IcebergSpark.registerBucketUDF(spark, "iceberg_buckets", DataTypes.StringType, 20)
    if (args.length == 3) {
      val df = spark.readStream
        .format("kafka")
        .options(
          Map(
            "kafka.bootstrap.servers" -> args(0),
            "subscribe" -> args(1),
            "startingOffsets" -> "earliest",
            "kafka.security.protocol" -> "SASL_SSL",
            "kafka.sasl.mechanism" -> "AWS_MSK_IAM",
            "kafka.sasl.jaas.config" -> "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "kafka.sasl.client.callback.handler.class" -> "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
          )
        )
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).as("data"))
        .select("data.*")
        .withColumnRenamed("updatedOn", "updated_on")
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
        .start()

      stream.awaitTermination()
      stream.stop()
      spark.stop()
    } else {
      println("Expected topic name")
      System.exit(-1)
    }
  }
}
