package main.scala.data

import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotator.{Stemmer, Tokenizer}
import main.scala.algorithm.AlgorithmUtils
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{explode, monotonically_increasing_id, not, regexp_replace, row_number}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DataframeCleaner(private val spark: SparkSession, private var df: DataFrame, private var N: Int) {
  println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " Start preprocessing...")

  /** Remove rows with not allowed topics */
  private val topics = TopicIndex.getTopicSeq
  this.df = this.df.filter(this.df.col("Context/Topic").isin(topics: _*))

  /** Remove rows with non-English sentences */
  private val notAlphDF = this.df.filter(not(this.df.col("Text").rlike("[a-zA-Z]")))
  private val accentsDF = this.df.filter(this.df.col("Text").rlike("[àáâãäåçèéêëìíîïòóôõöùúûü]"))

  this.df = this.df.except(notAlphDF.unionByName(accentsDF))

  /** Remove punctuations and multiple whitespace */
  this.df = this.df
    .withColumn("NoSymbols", regexp_replace(this.df.col("Text"), "[^a-zA-Z\\s]", ""))
    .drop("Text")
  this.df = this.df.withColumn("NoPunct", regexp_replace(this.df.col("NoSymbols"), "\\s+", " "))

  /** Create document assembler to work with stemmer */
  private val documentAssembler = new DocumentAssembler()
    .setInputCol("NoPunct")
    .setOutputCol("Document")

  /** Split the text in array of tokens */
  private val tokenizer = new Tokenizer()
    .setInputCols(Array(documentAssembler.getOutputCol))
    .setOutputCol("Tokens")

  /** Calculate words' stem */
  private val stemmer = new Stemmer()
    .setInputCols(Array(tokenizer.getOutputCol))
    .setOutputCol("Stem")
    .setLanguage("English")

  private val finisher = new Finisher()
    .setInputCols(Array(stemmer.getOutputCol))
    .setOutputCols(Array("Tokens"))

  /** Remove stop words */
  private val remover = new StopWordsRemover()
    .setInputCols(finisher.getOutputCols)
    .setOutputCols(Array("Text"))
  
  private val pipeline = new Pipeline()
    .setStages(Array(documentAssembler, tokenizer, stemmer, finisher, remover))

  this.df = pipeline.fit(df).transform(df)

  /** Add an index column for every sentence */
  this.df = df
    .withColumn("Index", row_number().over(Window.orderBy(monotonically_increasing_id())))
    .drop("NoSymbols", "NoPunct", "Tokens")

  println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " End preprocessing")

  def getDFMap: Map[String, DataFrame] = {
    AlgorithmUtils.splitDataFrame(this.df)
  }

  /** Get the transformed dataframe */
  def getPivotedDataFrame(trainDF: DataFrame): DataFrame = {
    println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " Start pivoting...")

    val maxPivots = 30000
    /** Increase max pivot in a dataframe to maxPivots and use case sensitive columns name */
    spark.conf.set("spark.sql.pivotMaxValues", maxPivots)
    spark.conf.set("spark.sql.caseSensitive", "true")

    /** Split a single sentence in multiple rows */
    var newDF = trainDF
      .select(trainDF.col("Index"), trainDF.col("Context/Topic"), explode(trainDF.col("Text")).as("Word"))
      .dropDuplicates("Index", "Word") // Get one occurrence even if a word appears more than one time in a sentence

    /** Remove blank characters */
    newDF = newDF.filter(!newDF.col("Word").rlike("^\\s*$"))

    /** Pivot the word column and make a count of words occurrences for every sentences */
    var pivotedDf = newDF
      .groupBy("Index", "Context/Topic")
      .pivot("Word")
      .count()
      .na.fill(0) //Fill NULL values with 0

    /** Remove word columns with count less than N */
    var wordCounts = newDF.groupBy("Word").count()
    wordCounts = wordCounts.filter(wordCounts.col("count") >= N)
    val columnsName = List("Index", "Context/Topic") ::: // Base column
      wordCounts.select("Word").as(Encoders.STRING).collect.toList // Word with more than N occurrence
    val columns = columnsName.map(name => pivotedDf.col(name))
    pivotedDf = pivotedDf.select(columns: _*) // Keep just the column selected

    println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " End pivoting")

    pivotedDf
  }

  /** Save in dataframe in a csv */
  def saveDataFrame(df: DataFrame, path: String): Unit = {
    df
      .write
      .option("header", value = true)
      .format("json")
      .mode("overwrite")
      .save(path)
  }
}
