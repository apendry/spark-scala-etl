package org.etl.sparkscala.common

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}


object DataUtils {

  def readCsv[T](path: String)
                (implicit spark: SparkSession,
                 encoder: Encoder[T]):
  Dataset[T] = {
    spark
      .read
      .option("header", false)
      .csv(path)
      .as[T]
  }

  def readParquet[T](path: String)
                    (implicit spark: SparkSession,
                     encoder: Encoder[T]):
  Dataset[T] = {
    spark
      .read
      .parquet(path)
      .as[T]
  }

}
