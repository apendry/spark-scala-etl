package org.etl.sparkscala.common.entrypoint

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSupportTest {

  private val sparkConf = new SparkConf()
  sparkConf.setMaster("local[*]")
  sparkConf.setAppName("SparkUnitTestApp")

  implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

}
