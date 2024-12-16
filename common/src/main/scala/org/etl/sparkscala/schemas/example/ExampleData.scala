package org.etl.sparkscala.schemas.example

case class ExampleData(dateHour: Long, id: Int, caloriesConsumed: Long, caloriesExpended: Long, activity: ExampleActivity)