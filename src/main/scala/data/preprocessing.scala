package data

import scala.io.Source
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, SparkSession}
class preprocessing {
  // Insert your dataset path here
  private val filePath = "./src/main/assets/Context.csv"

  private val spark = SparkSession
    .builder
    .appName("ClassifAI")
    .master("local[1]")
    .getOrCreate()

  private val df = spark.read.csv(this.filePath)

  def cleanDataset(): DataFrame = {
    df.show()
    df.printSchema()

    df
  }

  def removeStopWords(dataset: DataFrame): Unit = {

  }
}
