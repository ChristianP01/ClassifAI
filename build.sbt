import Dependencies._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "ClassifAI",
    libraryDependencies += sparkCore,
    libraryDependencies += sparkMLlib,
    libraryDependencies += spark_nlp
  )