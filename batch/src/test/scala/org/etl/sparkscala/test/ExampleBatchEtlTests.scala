package org.etl.sparkscala.test

import org.etl.sparkscala.entrypoint.SparkSupportTest
import org.etl.sparkscala.example.ExampleBatchEtlSupport.{ExampleOutput, PreparedInput, generateOutput, prepareInputData}
import org.etl.sparkscala.schemas.example
import org.etl.sparkscala.schemas.example.{ExampleData, ExampleHobby, ExampleMeta}
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExampleBatchEtlTests extends AnyFlatSpec with Matchers with Inside with SparkSupportTest {

  private val validHobbiesOne = Array(ExampleHobby("hobby1", 100), ExampleHobby("hobby2", 200))
  private val validHobbiesTwo = Array(ExampleHobby("hobby3", 300), ExampleHobby("hobby4", 400))

  "prepareInputData" should "filter inactive and empty hobbies" in {
    import spark.implicits._

    val validHobby = ExampleHobby("validHobby", 0)

    val testInput = spark.createDataset(Seq(
      ExampleData(1, true, Array(validHobby)),
      ExampleData(2, false, Array(validHobby)),
      ExampleData(3, true, Array()),
    )).as[ExampleData]

    val result = prepareInputData(testInput).collect().head

    inside(result) { case PreparedInput(id, hobbies, totalCost) =>
      id shouldBe 1
      totalCost shouldBe 0
      inside(hobbies.head) { case ExampleHobby(name, cost) =>
        name shouldBe "validHobby"
        cost shouldBe 0
      }
    }

  }

  it should "explode hobbies and re-collect correctly" in {
    import spark.implicits._

    val testInput = spark.createDataset(Seq(
      ExampleData(1, true, validHobbiesOne),
      ExampleData(2, true, validHobbiesTwo),
    )).as[ExampleData]

    val result = prepareInputData(testInput)
      .orderBy($"id".asc)
      .collect()

    inside(result.head) { case PreparedInput(id, hobbies, totalCost) =>
      id shouldBe 1
      totalCost shouldBe 300
      hobbies should contain theSameElementsAs validHobbiesOne
    }

    inside(result.last) { case PreparedInput(id, hobbies, totalCost) =>
      id shouldBe 2
      totalCost shouldBe 700
      hobbies should contain theSameElementsAs validHobbiesTwo
    }
  }

  "generateOutput" should "join meta and calculate percent cost" in {
    import spark.implicits._

    val inputMeta = spark.createDataset(Seq(
      ExampleMeta(1, "Kathryn", "Janeway", 41),
      ExampleMeta(2, "Tom", "Paris", 32)
    )).as[ExampleMeta]

    val preparedInput = spark.createDataset(Seq(
      PreparedInput(1, validHobbiesOne, 300),
      PreparedInput(2, validHobbiesTwo, 700)
    )).as[PreparedInput]

    val expectedResult = Array(
      ExampleOutput("Kathryn", "Janeway", "hobby1", 0.33),
      ExampleOutput("Kathryn", "Janeway", "hobby2", 0.67),
      ExampleOutput("Tom", "Paris", "hobby3", 0.43),
      ExampleOutput("Tom", "Paris", "hobby4", 0.57)
    )

    val result = generateOutput(preparedInput, inputMeta).collect()

    result should contain theSameElementsAs expectedResult
  }

}
