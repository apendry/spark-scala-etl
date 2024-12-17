package org.etl.sparkscala.test

import org.etl.sparkscala.entrypoint.SparkSupportTest
import org.etl.sparkscala.example.ExampleBatchEtlSupport.{ExampleOutput, PreparedInput, generateOutput, prepareInputData}
import org.etl.sparkscala.schemas.example
import org.etl.sparkscala.schemas.example.{ExampleCaloriesData, ExampleActivity, ExamplePersonMeta}
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExampleBatchEtlTests extends AnyFlatSpec with Matchers with Inside with SparkSupportTest {

  private val validHobbiesOne = Array(ExampleActivity("hobby1", 100), ExampleActivity("hobby2", 200))
  private val validHobbiesTwo = Array(ExampleActivity("hobby3", 300), ExampleActivity("hobby4", 400))

  "prepareInputData" should "filter inactive and empty hobbies" in {
    import spark.implicits._

    val validHobby = ExampleActivity("validHobby", 0)

    val testInput = spark.createDataset(Seq(
      ExampleCaloriesData(1, true, Array(validHobby)),
      ExampleCaloriesData(2, false, Array(validHobby)),
      ExampleCaloriesData(3, true, Array()),
    )).as[ExampleCaloriesData]

    val result = prepareInputData(testInput).collect().head

    inside(result) { case PreparedInput(id, hobbies, totalCost) =>
      id shouldBe 1
      totalCost shouldBe 0
      inside(hobbies.head) { case ExampleActivity(name, cost) =>
        name shouldBe "validHobby"
        cost shouldBe 0
      }
    }

  }

  it should "explode hobbies and re-collect correctly" in {
    import spark.implicits._

    val testInput = spark.createDataset(Seq(
      ExampleCaloriesData(1, true, validHobbiesOne),
      ExampleCaloriesData(2, true, validHobbiesTwo),
    )).as[ExampleCaloriesData]

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
      ExamplePersonMeta(1, "Kathryn", "Janeway", 41),
      ExamplePersonMeta(2, "Tom", "Paris", 32)
    )).as[ExamplePersonMeta]

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
