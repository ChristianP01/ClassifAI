package data

import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotator.{Stemmer, Tokenizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{not, regexp_replace}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class DataframeCleaner(private val spark: SparkSession, private var df: DataFrame) {
  /** Get the transformed dataframe */
  def getDataFrame: DataFrame = {
    df
  }

  println("Starting preprocessing phase 1")

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

  this.df = this.df.except(notAlphDF.unionByName(accentsDF))

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

  println("Ended preprocessing phase 1")
  println("Starting preprocessing phase 2")

  var wordList: List[String] = List.empty // List of words for columns-schema
  var sentenceList: List[Seq[String]] = List.empty // List of sentences to fill cells

  for (row <- this.df.select("Text").rdd.collect()) {

    val sentence = row.getSeq[String](0).toList
    sentenceList :+= sentence

    sentence.foreach( word => {
      if (!wordList.contains(word)) {
        wordList :+= word
      }
    })
  }

  var schema = new StructType()

  wordList.foreach(fieldName => {
    schema = schema.add(fieldName, IntegerType)
  })

  // Edit since, for each sentence in sentenceList, 1 if word is in else 0
  var cells: Array[Row] = Array.empty
  var tmpSeq: Array[Any] = Array.empty

  sentenceList.foreach(sentence => {
    tmpSeq = Array.fill(wordList.length)(0)

    sentence.foreach(word => {
      tmpSeq(wordList.indexOf(word)) = 1
    })
    cells = cells :+ Row.fromSeq(tmpSeq)
  })

  println("Ended preprocessing phase 2")
  println("Saving new preprocessed dataframe")

  val newdf: DataFrame = this.spark.createDataFrame(spark.sparkContext.parallelize(cells), schema)

  /**
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
  */
}