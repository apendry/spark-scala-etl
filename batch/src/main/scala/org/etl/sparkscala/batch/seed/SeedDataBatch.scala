package org.etl.sparkscala.batch.seed

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import SeedDataBatchSupport._
import org.etl.sparkscala.common.entrypoint.SparkSupport
import org.etl.sparkscala.common.example.{ExampleActivityMeta, ExampleCaloriesData, ExamplePersonMeta}
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalUnit}

/**
 * The following process is simply used to populate the test data that
 * downstream tasks require. It could mimic a log dump, application
 * output, or anything really as it's completely generic.
 *
 * The data being output will be randomly generated using scala gen
 * but will still adhere to the required downstream schemas expected.
 */
class SeedDataBatch extends SparkSupport {

  private val log: Logger = Logger.getLogger(this.getClass)

  private class ArgParse(arguments: Seq[String]) extends ScallopConf(arguments) {

    val calories = new Subcommand("calories") {
      val output: ScallopOption[String] = opt[String](required = true)
      val rows: ScallopOption[Long] = opt[Long](required = false, default = Some(1000L))
      val targetDate: ScallopOption[String] = opt[String](required = true)
    }

    val peopleMetaOutput: ScallopOption[String] = opt[String](required = false)
    val activityMetaOutput: ScallopOption[String] = opt[String](required = false)
    addSubcommand(calories)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val argParse = new ArgParse(args)

    val zonedTargetDateTime = ZonedDateTime.parse(argParse.calories.targetDate())

    //Generating Calories Seed Data
    if(argParse.calories.output.isDefined){
      val targetDateTime = zonedTargetDateTime.truncatedTo(ChronoUnit.HOURS)
      log.info(s"Running Calories Output, destination: ${argParse.calories.output()}")
      val exampleCaloriesOutput: Dataset[ExampleCaloriesData] = generateCaloriesData(argParse.calories.rows(), targetDateTime.toInstant.toEpochMilli)
      exampleCaloriesOutput.write.parquet(argParse.calories.output())
    }

    //Generating Person Seed Meta
    if(argParse.peopleMetaOutput.isDefined){
      val personMeta: Dataset[ExamplePersonMeta] = generatePersonMeta()
      personMeta.write.parquet(argParse.peopleMetaOutput())
    }

    //Generating Activity Seed Meta
    if(argParse.activityMetaOutput.isDefined){
      val activityMeta: Dataset[ExampleActivityMeta] = generateActivityMeta()
      activityMeta.write.parquet(argParse.activityMetaOutput())
    }

  }

}
