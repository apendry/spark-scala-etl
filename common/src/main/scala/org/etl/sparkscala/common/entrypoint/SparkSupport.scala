package org.etl.sparkscala.common.entrypoint

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

abstract class SparkSupport(sparkConf: SparkConf = new SparkConf()) {

    def this(conf: Map[String, String]) = this({
        val mappedSparkConf = new SparkConf()
        mappedSparkConf.setAll(conf)
    })

    //Default Master to local if not specified
    sparkConf.setMaster(sparkConf.getOption("master").getOrElse("local[*]"))

    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

}
