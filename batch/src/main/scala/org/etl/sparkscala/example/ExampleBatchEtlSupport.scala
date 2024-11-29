package org.etl.sparkscala.example

import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition.first
import org.apache.spark.sql.functions.{array, explode, size, sum}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.etl.sparkscala.schemas.example.{ExampleData, ExampleHobby, ExampleMeta}

object ExampleBatchEtlSupport {

  case class PreparedInput(id: Int, hobbies: Array[ExampleHobby], totalCost: BigDecimal)
  case class ExampleOutput(firstName: String, lastName: String, hobbyName: String, percentCost: BigDecimal)


  def prepareInputData(inputData: Dataset[ExampleData])
                      (implicit spark: SparkSession):
  Dataset[PreparedInput] = {
    import spark.implicits._

    inputData
      .filter($"active" && size($"hobbies") > 0)
      .agg(explode($"hobbies").as("hobby"))
      .select(
        $"id",
        $"hobby.name".as("name"),
        $"hobby.cost".as("cost")
      ).groupBy($"id")
      .agg(
        array($"name", $"cost").as("hobbies"),
        sum($"cost").as("totalCost")
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
        $"id",
        "inner"
      )
      .agg(explode($"hobbies").as("hobby"))
      .select(
        $"firstName",
        $"lastName",
        $"hobby.name".as("hobbyName"),
        ($"hobby.cost" / $"cost").as("percentCost")
      ).as[ExampleOutput]
  }


}
