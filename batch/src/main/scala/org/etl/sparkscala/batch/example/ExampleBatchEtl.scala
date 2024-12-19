package org.etl.sparkscala.batch.example

import org.etl.sparkscala.common.DataUtils.{readCsv, readParquet}
import ExampleBatchEtlSupport.{generateOutput, prepareInputData}
import org.etl.sparkscala.common.entrypoint.SparkSupport
import org.etl.sparkscala.common.example.{ExampleActivityMeta, ExampleCaloriesData, ExamplePersonMeta}
import org.rogach.scallop.{ScallopConf, ScallopOption}

object ExampleBatchEtl extends SparkSupport {

  private class ArgParse(arguments: Seq[String]) extends ScallopConf(arguments) {
    val inputData: ScallopOption[String] = opt[String](required = true)
    val inputPersonMeta: ScallopOption[String] = opt[String](required = true)
    val inputActivityMeta: ScallopOption[String] = opt[String](required = true)
    val output: ScallopOption[String] = opt[String](required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val argParse = new ArgParse(args)

    //Extract
    val exampleData = readParquet[ExampleCaloriesData](argParse.inputData())
    val examplePersonMeta = readParquet[ExamplePersonMeta](argParse.inputPersonMeta())
    val exampleActivityMeta = readParquet[ExampleActivityMeta](argParse.inputActivityMeta())

    //Transform
    val preparedInput = prepareInputData(exampleData)
    val output = generateOutput(preparedInput, examplePersonMeta, exampleActivityMeta)

    //Load
    output.write.parquet(argParse.output())

  }

}
