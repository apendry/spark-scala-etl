package org.etl.sparkscala.seed

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.etl.sparkscala.entrypoint.SparkSupport
import org.etl.sparkscala.schemas.example.{ExampleActivityMeta, ExampleCaloriesData, ExamplePersonMeta}
import org.etl.sparkscala.seed.SeedDataBatchSupport._
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

/**
 * The following process is simply used to populate the test data that
 * downstream tasks require. It could mimic a log dump, application
 * output, or anything really as it's completely generic.
 *
 * The data being output will be randomly generated using scala gen
 * but will still adhere to the required downstream schemas expected.
 */
object SeedDataBatch extends SparkSupport {

  private val log: Logger = Logger.getLogger(SeedDataBatch.getClass)

  private class ArgParse(arguments: Seq[String]) extends ScallopConf(arguments) {
    val calories = new Subcommand("calories") {
      val output: ScallopOption[String] = opt[String](required = true)
      val rows: ScallopOption[Long] = opt[Long](required = false)
      val targetDate: ScallopOption[Long] = opt[Long](required = false)
    }

    val peopleMetaOutput: ScallopOption[String] = opt[String](required = false)
    val activityMetaOutput: ScallopOption[String] = opt[String](required = false)
    addSubcommand(calories)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val argParse = new ArgParse(args)

    //Generating Calories Seed Data
    if(argParse.calories.output.isDefined){
      log.info(s"Running Calories Output, destination: ${argParse.calories.output()}")
      val exampleCaloriesOutput = generateData(argParse.calories.rows.toOption, argParse.calories.targetDate.toOption)[ExampleCaloriesData]
      exampleCaloriesOutput.write.parquet(argParse.calories.output())
    }

    //Generating Person Seed Meta
    if(argParse.peopleMetaOutput.isDefined){
      generateData[ExamplePersonMeta]
    }

    //Generating Activity Seed Meta
    if(argParse.activityMetaOutput.isDefined){
      generateData[ExampleActivityMeta]
    }

  }

}
