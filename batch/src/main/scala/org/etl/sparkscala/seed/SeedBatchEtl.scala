package org.etl.sparkscala.seed

import org.etl.sparkscala.entrypoint.SparkSupport
import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * The following ETL is simply used to populate the test data that
 * downstream tasks require. It could mimic a log dump, application
 * output, or anything really as it's completely generic.
 *
 * The data being output will be randomly generated using scala gen
 * but will still adhere to the required downstream schemas expected.
 */
object SeedBatchEtl extends SparkSupport {

  private class ArgParse(arguments: Seq[String]) extends ScallopConf(arguments) {
    val output: ScallopOption[String] = opt[String](required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {

  }

}
