lazy val sparkVersion = "3.2.0"
lazy val sparkTestVersion = "3.2.19"

ThisBuild / organization := "org.etl.sparkscala"
ThisBuild / scalaVersion := "2.12.18"

lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.rogach" %% "scallop" % "5.1.0",
  "org.scalactic" %% "scalactic" % sparkTestVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  "org.scalatest" %% "scalatest" % sparkTestVersion % "test"
)

lazy val global = project
  .in(file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    common,
    batch
  )

lazy val common = (project in file("common"))
  .settings(
    version := "0.0.1-SNAPSHOT",
    libraryDependencies ++= commonDependencies ++ Seq()
  )
  .disablePlugins(AssemblyPlugin)

lazy val batch = (project in file("batch"))
  .dependsOn(common)
  .settings(
    assemblySettings,
    version := "0.0.1-SNAPSHOT",
    libraryDependencies ++= commonDependencies ++ Seq()
  )

lazy val assemblySettings = Seq(
  assembly / test := (Test / test).value,
  assembly / assemblyJarName := s"spark-scala-etl-${name.value}-${version.value}.jar"
)