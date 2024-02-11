package data

import breeze.optimize.DiffFunction.castOps
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.functions.first
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer, Word2Vec}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{explode, lit, not, regexp_replace}
import org.apache.spark.sql.types.IntegerType

import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.Vector

class CleanedDataFrame(private val spark: SparkSession, private var df: DataFrame) {

  this
    .removeTopicErrors()
    .removeForeignSentences()
    .removePunctuations()
    .tokenize()
    .removeStopWords()
    .wordOccurs()
//    .termFrequency()
//    .inverseDocumentFrequency()
//    .word2Vec()

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

  private def termFrequency(): CleanedDataFrame = {
    val hashingTF = new HashingTF()
      .setInputCol("Text")
      .setOutputCol("TF")

    this.df = hashingTF.transform(this.df)

    this
  }

  private def inverseDocumentFrequency(): CleanedDataFrame = {

    val idf: IDF = new IDF()
      .setMinDocFreq(5)
      .setInputCol("TF")
      .setOutputCol("IDF")

    idf.fit(this.df).transform(this.df)

    this
  }

  /**
   * For each word in each row of the "old" DataFrame, create a new row.
   * Group them to have their count.
   * Remove rows containing words with a count < 3 (not-so-informative words).
   */
  private def wordOccurs(): CleanedDataFrame = {

    // Creates DataFrame with a new row for each word in a sentence, then groups it to count them
    var wordsDF: DataFrame = this.df.select(explode(this.df.col("Text")) as "words")
    wordsDF = wordsDF.groupBy("words").count()
    wordsDF = wordsDF.filter(wordsDF.col("count") > 10)
    println(wordsDF.count())

    // Pivot rows to columns and remove first row
    wordsDF = wordsDF.groupBy().pivot("words").agg(first("count"))

    wordsDF.show()

    this.df = wordsDF

    this
  }

  /**
   * Creates a new DataFrame from the actual one, starting with the label column only.
   * Create a binary representation of each sentence tagging as 1 the words they contain.
   */
  private def binarizeSenteces(): CleanedDataFrame = {
    this
  }

  private def word2Vec(): CleanedDataFrame = {
    val w2v = new Word2Vec()
      .setInputCol("Text")
      .setOutputCol("Word2Vec")

    val w2vModel = w2v.fit(this.df)

    this.df = w2vModel.transform(this.df)

    this
  }

}
