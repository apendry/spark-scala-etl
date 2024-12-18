lazy val sparkVersion = "3.2.0"
lazy val scalaMinorVersion = "2.12.18"
lazy val sparkTestVersion = "3.2.19"

ThisBuild / organization := "org.etl.sparkscala"
ThisBuild / scalaVersion := scalaMinorVersion
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / assembly / assemblyJarName := s"spark-scala-etl-uber-${(ThisBuild / version).value}.jar"
ThisBuild / assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
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