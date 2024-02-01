package data

import scala.io.Source
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{regexp_replace, udf}

class CleanedDataFrame() {
  // Insert your dataset path here
  private val filePath = "./src/main/assets/Context.csv"

  private val spark = SparkSession
    .builder
    .appName("ClassifAI")
    .master("local[1]")
    .getOrCreate()

  private var df = spark.read.option("header", value = true).csv(this.filePath)

  def removeTopicErrors(): CleanedDataFrame = {

    val topics = List(
      "Animals",
      "Compliment",
      "Education",
      "Health",
      "Heavy Emotion",
      "Joke",
      "Love",
      "Politics",
      "Religion",
      "Science",
      "Self"
    )

    this.df = df.filter(df.col("Context/Topic").isin(topics: _*))

    this
  }

  def removePunctuations(): CleanedDataFrame = {
    this.df = df
      .withColumn("NoSymbols", regexp_replace(df.col("Text"), "[^a-zA-Z\\s]", ""))
      .drop("Text")

    this.df = df
      .withColumn("NoPunct", regexp_replace(df.col("NoSymbols"), "\\s+", " "))
      .drop("NoSymbols")

    this
  }

  def tokenize(): CleanedDataFrame = {
    val tokenizer = new Tokenizer()
      .setInputCol("NoPunct")
      .setOutputCol("Tokens")

    this.df = tokenizer
      .transform(this.df)
      .drop("NoPunct")

    this
  }

  def removeStopWords(): CleanedDataFrame = {
    val remover = new StopWordsRemover()
      .setInputCol("Tokens")
      .setOutputCol("Filtered")

    this.df = remover
      .transform(this.df)
      .drop("Tokens")

    this.df.show()
    this
  }
}
