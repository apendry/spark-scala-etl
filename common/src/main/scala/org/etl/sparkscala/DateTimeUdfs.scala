package org.etl.sparkscala

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

object DateTimeUdfs {

  val defaultDateFormatter: DateTimeFormatter = ISO_OFFSET_DATE_TIME

  def longToZonedDateTime(zone: ZoneId = ZoneId.of("UTC"))(epoch: Long):
  ZonedDateTime = {
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(epoch), zone)
  }

  def longToZonedDateTimeUdf(zone: ZoneId): UserDefinedFunction = udf[ZonedDateTime, Long](
    longToZonedDateTime(zone)
  )

  def longToZonedDateTimeUdf(): UserDefinedFunction = udf[ZonedDateTime, Long](
    longToZonedDateTime()
  )

  def longToZonedFormattedDateString(zone: ZoneId = ZoneId.of("UTC"), fmt: DateTimeFormatter = defaultDateFormatter)(epoch: Long):
  String = {
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(epoch), zone).format(fmt)
  }

  def longToZonedFormattedDateStringUdf(zone: ZoneId, fmt: DateTimeFormatter): UserDefinedFunction = udf[String, Long](
    longToZonedFormattedDateString(zone, fmt)
  )

  def longToZonedFormattedDateStringUdf(zone: ZoneId): UserDefinedFunction = udf[String, Long](
    longToZonedFormattedDateString(zone = zone)
  )

  def longToZonedFormattedDateStringUdf(fmt: DateTimeFormatter): UserDefinedFunction = udf[String, Long](
    longToZonedFormattedDateString(fmt = fmt)
  )

  def longToZonedFormattedDateStringUdf(): UserDefinedFunction = udf[String, Long](
    longToZonedFormattedDateString()
  )


}
