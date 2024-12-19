package org.etl.sparkscala

import org.etl.sparkscala.common.DateTimeUdfs.{longToZonedDateTime, longToZonedFormattedDateString}

import java.time.{ZoneId, ZonedDateTime}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.format.DateTimeFormatter

class DataUtilsTest extends AnyFlatSpec with Matchers {

  private val defaultEpochTimestamp = 1704067200000L
  private val defaultZonedDateTime = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"))
  private val defaultZonedDateTimeString = "2024-01-01T00:00:00Z"

  "longToZonedDateTime" should "parse epoch with default zone" in {
    longToZonedDateTime()(defaultEpochTimestamp) shouldBe defaultZonedDateTime
  }

  it should "parse epoch with specified zone" in {
    val edtZoneId = ZoneId.of("America/Toronto")
    val edtZonedDateTime = ZonedDateTime.of(2023, 12, 31, 19, 0, 0, 0, edtZoneId)

    longToZonedDateTime(edtZoneId)(defaultEpochTimestamp) shouldBe edtZonedDateTime
  }

  "longToZonedFormattedDateString" should "parse epoch with default zone and format" in {
    longToZonedFormattedDateString()(defaultEpochTimestamp) shouldBe defaultZonedDateTimeString
  }

  it should "parse epoch with default zone and user format" in {
    val userFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:SS")
    longToZonedFormattedDateString(fmt = userFormat)(defaultEpochTimestamp) shouldBe "2024-01-01 00:00:00"
  }

  it should "parse epoch with user zone and default format" in {
    val edtZoneId = ZoneId.of("America/Toronto")
    longToZonedFormattedDateString(zone = edtZoneId)(defaultEpochTimestamp) shouldBe "2023-12-31T19:00:00-05:00"
  }

  it should "parse epoch with user zone and format" in {
    val edtZoneId = ZoneId.of("America/Toronto")
    val userFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:SS")
    longToZonedFormattedDateString(zone = edtZoneId, fmt = userFormat)(defaultEpochTimestamp) shouldBe "2023-12-31 19:00:00"

  }

}
