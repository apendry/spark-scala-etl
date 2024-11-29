package org.etl.sparkscala.schemas.example

case class ExampleData(id: Long, active: Boolean, hobbies: Array[ExampleHobby])