package org.example.spark

import org.apache.spark.sql.SparkSession

/**
 * Sample test used to retrieve the environment variables from the Job.
 */
object EnvRetrieverTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    System.getenv().forEach((name, value) => {
      println(s"Variable '$name': '$value'")
    })
    System.getProperties.forEach((name, value) => {
      println(s"Property '$name': '$value'")
    })
    spark.stop()
  }
}
