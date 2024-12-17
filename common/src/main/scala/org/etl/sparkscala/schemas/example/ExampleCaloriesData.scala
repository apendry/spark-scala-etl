package org.etl.sparkscala.schemas.example

case class ExampleCaloriesData(dateHour: Long, id: Int, caloriesConsumed: Long, caloriesExpended: Long, activity: ExampleActivity)