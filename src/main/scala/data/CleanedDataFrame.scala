package data

import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{not, regexp_replace}

class CleanedDataFrame(private val spark: SparkSession, private var df: DataFrame) {

  this.removeTopicErrors().removeForeignSentences().removePunctuations().tokenize().removeStopWords()

  /** Get the transformed dataframe */
  def getDataFrame: DataFrame = {
    df
  }

  /** Remove rows with not allowed topics */
  private def removeTopicErrors(): CleanedDataFrame = {
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

  /** Remove rows with non-English sentences */
  private def removeForeignSentences(): CleanedDataFrame = {
    val notAlph: String = "[a-zA-Z]"
    val accents: String = "[àáâãäåçèéêëìíîïòóôõöùúûü]"
    val notAlphDF = df.filter(not(df.col("Text").rlike(notAlph)))
    val accentsDF = df.filter(df.col("Text").rlike(accents))
    val foreignDF = notAlphDF.unionByName(accentsDF, allowMissingColumns = true)

    this.df = this.df.except(foreignDF)

    this
  }

  /** Remove punctuations and multiple whitespace */
  private def removePunctuations(): CleanedDataFrame = {

    this.df = df
      .withColumn("NoSymbols", regexp_replace(df.col("Text"), "[^a-zA-Z\\s]", ""))
      .drop("Text")

    this.df = df
      .withColumn("NoPunct", regexp_replace(df.col("NoSymbols"), "\\s+", " "))
      .drop("NoSymbols")

    this
  }

  /** Transform texts in list of tokens */
  private def tokenize(): CleanedDataFrame = {
    val tokenizer = new Tokenizer()
      .setInputCol("NoPunct")
      .setOutputCol("Tokens")

    this.df = tokenizer
      .transform(this.df)
      .drop("NoPunct")

    this
  }

  /** Remove stop words */
  private def removeStopWords(): CleanedDataFrame = {
    val remover = new StopWordsRemover()
      .setInputCol("Tokens")
      .setOutputCol("Text")

    this.df = remover
      .transform(this.df)
      .drop("Tokens")

    this
  }
}
