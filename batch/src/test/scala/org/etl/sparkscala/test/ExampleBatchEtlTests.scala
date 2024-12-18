package org.etl.sparkscala.test

import org.etl.sparkscala.entrypoint.SparkSupportTest
import org.etl.sparkscala.example.ExampleBatchEtlSupport.{ExampleOutput, PreparedInput, generateOutput, prepareInputData}
import org.etl.sparkscala.schemas.example
import org.etl.sparkscala.schemas.example.{ExampleCaloriesData, ExampleActivity, ExamplePersonMeta}
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExampleBatchEtlTests extends AnyFlatSpec with Matchers with Inside with SparkSupportTest {

  private val validHobbiesOne = Array(ExampleActivity(1, 100), ExampleActivity(2, 200))
  private val validHobbiesTwo = Array(ExampleActivity(3, 300), ExampleActivity(4, 400))


}
