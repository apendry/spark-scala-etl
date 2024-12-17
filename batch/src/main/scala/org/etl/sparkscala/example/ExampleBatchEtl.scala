package org.etl.sparkscala.example

import org.etl.sparkscala.DataUtils.{readCsv, readParquet}
import org.etl.sparkscala.entrypoint.SparkSupport
import org.etl.sparkscala.example.ExampleBatchEtlSupport.{generateOutput, prepareInputData}
import org.etl.sparkscala.schemas.example.{ExampleActivityMeta, ExampleCaloriesData, ExamplePersonMeta}
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
