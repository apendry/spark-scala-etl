ThisBuild / organization := "org.etl.sparkscala"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0-SNAPSHOT"

lazy val sparkVersion = "3.2.0"
lazy val sparkTestVersion = "3.2.19"

lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.rogach" %% "scallop" % "5.1.0",
  "org.scalactic" %% "scalactic" % sparkTestVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  "org.scalatest" %% "scalatest" % sparkTestVersion % "test"
)

lazy val common = (project in file("common"))
  .settings(
    name := "common",
    version := "1.0.0",
    libraryDependencies ++= commonDependencies ++ Seq()
  )

lazy val batch = (project in file("batch"))
  .dependsOn(common)
  .settings(
    name := "batch",
    version := "1.0.0",
    libraryDependencies ++= commonDependencies ++ Seq()
  )