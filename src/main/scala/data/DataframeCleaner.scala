package data

import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotator.{Stemmer, Tokenizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, explode, monotonically_increasing_id, not, regexp_replace, row_number, when}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class DataframeCleaner(private val spark: SparkSession, private var df: DataFrame) {
  /** Get the transformed dataframe */
  def getDataFrame: DataFrame = {
    this.df
  }

  println("Start preprocessing")

  /** Remove rows with not allowed topics */
  private val topics = List(
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

  /** Remove rows with wrong topic (parsing fault) */
  this.df = this.df.filter(this.df.col("Context/Topic").isin(topics: _*))

  // Remove rows with non-English sentences
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

  println("End preprocessing")
  println("Start pivoting")

  private val maxPivots = 30000
  /** Increase max pivot in a dataframe to maxPivots */
  spark.conf.set("spark.sql.pivotMaxValues", maxPivots)

  /** Add an index column for every sentence */
  this.df = df.withColumn("Index", row_number().over(Window.orderBy(monotonically_increasing_id())))

  /** Split a single sentence in multiple rows */
  this.df = this.df.select(this.df.col("Index"), this.df.col("Context/Topic"), explode(this.df.col("Text")).as("Word"))

  /** Remove blank characters */
  this.df = this.df.filter(!this.df.col("Word").rlike("^\\s*$"))

  /** Remove sparse words with count less than N */
  private var wordCounts = this.df.groupBy("Word").count()
  private val N = 2
  wordCounts = wordCounts.filter(wordCounts.col("count") >= N)
  this.df = this.df.join(wordCounts, "Word")

  /** Pivot the word column and make a count of words occurrences for every sentences */
  this.df = this.df
    .groupBy("Index", "Context/Topic")
    .pivot("Word")
    .count()
    .drop("Index") //Remove Index column
    .na.fill(0) //Fill NULL values with 0

  println("End pivoting")
  println("Start saving")

  this.df
    .write
    .option("header",value = true)
    .format("csv")
    .mode("overwrite")
    .save(System.getProperty("user.dir") + "/output/")

  println("End saving")
}