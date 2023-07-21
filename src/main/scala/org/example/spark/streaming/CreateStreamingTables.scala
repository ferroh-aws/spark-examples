package org.example.spark.streaming

import org.apache.spark.sql.SparkSession

object CreateStreamingTables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS glue.icebergdb")
    spark.sql("DROP TABLE IF EXISTS glue.icebergdb.streaming_txs PURGE")
    spark.sql(
      """CREATE TABLE IF NOT EXISTS glue.icebergdb.streaming_txs (
        |id string,
        |updated_on timestamp,
        |city string,
        |zip string,
        |commerce string,
        |amount decimal(10,2),
        |status string
        |) USING iceberg
        |PARTITIONED BY (bucket(20, id))
        |location 's3://data.aws-poc-hf.com/iceberg/db/streaming_txs'
        |""".stripMargin)
    spark.stop()
  }
}
