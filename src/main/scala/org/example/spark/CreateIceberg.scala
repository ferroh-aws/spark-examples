package org.example.spark

import org.apache.spark.sql.SparkSession

object CreateIceberg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS glue.icebergdb")
    spark.sql(
      """CREATE TABLE IF NOT EXISTS glue.icebergdb.customers (
        |id string,
        |first_name string,
        |last_name string,
        |updated timestamp,
        |zip string,
        |street string,
        |city string,
        |card string
        |) USING iceberg
        |PARTITIONED BY (bucket(20, id))
        |location 's3://data.aws-poc-hf.com/iceberg/db/customers'
        |""".stripMargin)
    spark.sql(
      """CREATE TABLE IF NOT EXISTS glue.icebergdb.transactions (
        |id string,
        |op_ts timestamp,
        |city string,
        |zip string,
        |commerce string,
        |amount decimal(10,2),
        |status string
        |) USING iceberg
        |PARTITIONED BY (date(op_ts))
        |location 's3://data.aws-poc-hf.com/iceberg/db/transactions'
        |""".stripMargin)
    spark.stop()
  }
}
