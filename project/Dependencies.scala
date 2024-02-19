import sbt._

object Dependencies {
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "3.5.0"
  lazy val sparkMLlib = "org.apache.spark" %% "spark-mllib" % "3.5.0"
  lazy val spark_nlp = "com.johnsnowlabs.nlp" % "spark-nlp_2.12" % "5.2.3"
}
