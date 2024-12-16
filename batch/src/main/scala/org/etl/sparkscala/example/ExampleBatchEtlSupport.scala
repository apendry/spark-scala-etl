package org.etl.sparkscala.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.etl.sparkscala.schemas.example.{ExampleActivity, ExampleActivityMeta, ExampleData, ExamplePersonMeta}

object ExampleBatchEtlSupport {

  case class PreparedInput(id: Long, hobbies: Array[ExampleActivity], totalCost: BigDecimal)
  case class ExampleOutput(firstName: String, lastName: String, hobbyName: String, percentCost: BigDecimal)


  def prepareInputData(inputData: Dataset[ExampleData])
                      (implicit spark: SparkSession):
  Dataset[PreparedInput] = ???

  def generateOutput(preparedInput: Dataset[PreparedInput],
                     inputPersonMeta: Dataset[ExamplePersonMeta],
                     inputActivityMeta: Dataset[ExampleActivityMeta])
                    (implicit spark: SparkSession):
  Dataset[ExampleOutput] = ???


}
