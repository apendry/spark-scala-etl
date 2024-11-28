package org.etl.sparkscala

import org.etl.sparkscala.DateTimeUdfs.longToZonedDateTime
import org.etl.sparkscala.entrypoint.SparkSupportTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{ZoneId, ZonedDateTime}


class DataUtilsTest extends AnyFlatSpec with Matchers with SparkSupportTest {

  private val defaultEpochTimestamp = 1704114000000L
  private val defaultZonedDateTime = ZonedDateTime.of(2024, 1, 1, 1, 0, 0, 0, ZoneId.of("UTC"))

  "DateUtils" should "parse epoch with default zone" in {
    longToZonedDateTime()(defaultEpochTimestamp) shouldBe defaultZonedDateTime
  }

  it should "parse epoch with specified zone" in {
    1 == 1
  }

  it should "parse epoch with default zone and format" in {

  }

  it should "parse epoch with default zone and user format" in {

  }

  it should "parse epoch with user zone and default format" in {

  }

  it should "parse epoch with user zone and format" in {

  }

}
