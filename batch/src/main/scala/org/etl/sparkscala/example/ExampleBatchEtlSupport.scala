package org.etl.sparkscala.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.etl.sparkscala.schemas.example.{ExampleData, ExampleHobby, ExampleMeta}

object ExampleBatchEtlSupport {

  case class PreparedInput(id: Long, hobbies: Array[ExampleHobby], totalCost: BigDecimal)
  case class ExampleOutput(firstName: String, lastName: String, hobbyName: String, percentCost: BigDecimal)


  def prepareInputData(inputData: Dataset[ExampleData])
                      (implicit spark: SparkSession):
  Dataset[PreparedInput] = {
    import spark.implicits._

    inputData
      .filter($"active" && size($"hobbies") > 0)
      .select(
        $"id",
        explode($"hobbies").as("hobby")
      ).groupBy($"id")
      .agg(
        collect_set(struct($"hobby.name", $"hobby.cost")).as("hobbies"),
        sum($"hobby.cost").as("totalCost")
      ).as[PreparedInput]

  }

  def generateOutput(preparedInput: Dataset[PreparedInput],
                     inputMeta: Dataset[ExampleMeta])
                    (implicit spark: SparkSession):
  Dataset[ExampleOutput] = {
    import spark.implicits._

    preparedInput
      .join(
        inputMeta,
        Seq("id"),
        "inner"
      ).withColumn("hobby", explode($"hobbies"))
      .select(
        $"firstName",
        $"lastName",
        $"hobby.name".as("hobbyName"),
        round($"hobby.cost" / $"totalCost", 2).as("percentCost")
      ).as[ExampleOutput]
  }


}
