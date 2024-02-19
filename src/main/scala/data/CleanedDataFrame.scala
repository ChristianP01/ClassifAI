package data

import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotator.{Stemmer, Tokenizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{coalesce, col, explode, lit, not, regexp_replace}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, SparkSession}

class CleanedDataFrame(private val spark: SparkSession, private var df: DataFrame) {
  /** Get the transformed dataframe */
  def getDataFrame: DataFrame = {
    df
  }

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
  this.df = df.filter(df.col("Context/Topic").isin(topics: _*))

  // Remove rows with non-English sentences
  private val notAlphDF = df.filter(not(df.col("Text").rlike("[a-zA-Z]")))
  private val accentsDF = df.filter(df.col("Text").rlike("[àáâãäåçèéêëìíîïòóôõöùúûü]"))

  this.df = this.df.except(notAlphDF.unionByName(accentsDF, allowMissingColumns = true))

  /** Remove punctuations and multiple whitespace */
  this.df = df
    .withColumn("NoSymbols", regexp_replace(df.col("Text"), "[^a-zA-Z\\s]", ""))
    .drop("Text")

  this.df = df
    .withColumn("NoPunct", regexp_replace(df.col("NoSymbols"), "\\s+", " "))
    .drop("NoSymbols")

  /** Create document assembler to work with stemmer */
  private val documentAssembler = new DocumentAssembler()
    .setInputCol("NoPunct")
    .setOutputCol("document")

  /** Split the text in array of tokens */
  private val tokenizer = new Tokenizer()
    .setInputCols(Array(documentAssembler.getOutputCol))
    .setOutputCol("tokens")

  /** Calculate words' stem */
  private val stemmer = new Stemmer()
    .setInputCols(Array(tokenizer.getOutputCol))
    .setOutputCol("stem")
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

  /**
    * For each word in each row of the "old" DataFrame, create a new row with relative count-per-topic.
    */
  private val topicWordCount = df
    .select(this.df.col("Context/Topic"), explode(this.df.col("Text")).as("Word"))
    .groupBy("Word", "Context/Topic")
    .count()

  /** Pivoting dataset with topics as cols */
  private val pivoted = topicWordCount.groupBy("Word").pivot("Context/Topic").sum("count")

  this.df = topics.foldLeft(pivoted) { (data, column) =>
    data.withColumn(column, coalesce(col(column), lit(0)))
  }

  /** Add a column with sum of a word's occurrences */
  this.df = df.withColumn("Total", df.columns.drop(1).map(df(_)).reduce(_ + _))
}