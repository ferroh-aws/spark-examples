package org.example.spark.streaming

import org.apache.spark.sql.SparkSession

import java.time.Instant
import java.time.temporal.ChronoUnit

object TableMaintenance {
  def main(args: Array[String]): Unit = {
    if (args.length == 5) {
      val spark = SparkSession
        .builder()
        .getOrCreate()
      val catalogName = args(0)
      val databaseName = args(1)
      val tableName = args(2)
      val seconds = args(3).toInt
      val size = args(4).toInt
      val expire = Instant.now().minus(seconds, ChronoUnit.SECONDS)
      spark.sql(
        s"""
           |CALL $catalogName.system.expire_snapshots('$databaseName.$tableName', TIMESTAMP '${expire.toString}')
           |""".stripMargin)
      spark.sql(
        s"""
           |CALL $catalogName.system.rewrite_data_files('$databaseName.$tableName')
           |""".stripMargin)
      spark.sql(
        s"""
           |CALL $catalogName.system.rewrite_manifests('$databaseName.$tableName')
           |""".stripMargin)
      spark.sql(
        s"""
           |CALL $catalogName.system.remove_orphan_files('$databaseName.$tableName')
           |""".stripMargin)
      spark.stop()
    } else {
      println("Expecting database, table name and seconds to expire snapshots.")
      System.exit(-1)
    }
  }
}
