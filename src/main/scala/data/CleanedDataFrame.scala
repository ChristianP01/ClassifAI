package data

import org.apache.spark.sql.functions.{coalesce, col, explode, lit, not, regexp_replace}
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.feature.Stemmer

class CleanedDataFrame(private val spark: SparkSession, private var df: DataFrame) {

  this
    .removeTopicErrors()
    .removeForeignSentences()
    .removePunctuations()
    .tokenize()
    .removeStopWords()
    .stemming()
    .wordOccurs()

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

  private def stemming(): CleanedDataFrame = {
//    val documentAssembler = new DocumentAssembler()
//      .setInputCol("Text")
//      .setOutputCol("Document")
//      .setCleanupMode("shrink")

//    this.df = documentAssembler.transform(this.df)

    val stemmer = new Stemmer()
      .setInputCol("Text")
      .setOutputCol("Stem")
      .setLanguage("English")

    this.df = stemmer.transform(this.df)

//    val finisher = new Finisher()
//      .setInputCols("Stem")

//    val pipeline = new Pipeline()
//      .setStages(Array(documentAssembler))
//
//    this.df = pipeline.fit(df).transform(df)
    df.show()

    this
  }

  /**
   * For each word in each row of the "old" DataFrame, create a new row.
   * Group them to have their count.
   */
  private def wordOccurs(): CleanedDataFrame = {
    val topics = df.select("Context/Topic").distinct.collect.flatMap(_.toSeq)

    val topicWordCount = df
      .select(this.df.col("Context/Topic"), explode(this.df.col("Text")).as("Word"))
      .groupBy("Word", "Context/Topic")
      .count()

    val pivoted = topicWordCount.groupBy("Word").pivot("Context/Topic").sum("count")

    val result = topics.foldLeft(pivoted) { (data, column) =>
      data.withColumn(column.toString, coalesce(col(column.toString), lit(0)))
    }

    result.show()

    this
  }
}
