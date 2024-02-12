import Dependencies._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "ClassifAI",
    libraryDependencies += sparkCore,
    libraryDependencies += sparkMLlib,
    libraryDependencies += stemmer
  )