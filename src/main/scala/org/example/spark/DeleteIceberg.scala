package org.example.spark

import org.apache.spark.sql.SparkSession

object DeleteIceberg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    spark.sql("DROP TABLE glue.icebergdb.customers PURGE")
    spark.sql("DROP TABLE glue.icebergdb.transactions PURGE")
    spark.stop()
  }
}
