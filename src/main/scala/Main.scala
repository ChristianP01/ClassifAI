import data.DataframeCleaner
import algorithm.{MapReduceAlgorithm, SeqAlgorithm}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Main {
  def main(args: Array[String]): Unit = {
    /** Start Spark session */
    val spark = SparkSession
      .builder
      .appName("ClassifAI")
      .master("local[*]")
      .getOrCreate()

    /**
     * Execute preprocessing and save data frame.
     * Change isDFNew to execute or skip this process.
     *
     * true -> evaluate 
     * false -> skip
     */
    val isDFNew: Boolean = false
    if (isDFNew) {
      /** Upload dataframe */
      val originalDF = spark.read.option("header", value = true).csv(System.getProperty("user.dir") +
        "/src/main/assets/Context.csv")

      /** Preprocess dataframe */
      val preprocessor = new DataframeCleaner(spark, originalDF)

      /** Saving preprocessed and pivoted df to apply spark transformation and optimize execution time */
      preprocessor.saveDataFrame(preprocessor.getPivotedDataFrame)
    }

    /** Read preprocessed dataframe */
    val pivotedDF = spark.read.option("header", value = true).csv(System.getProperty("user.dir") + "/output/")

    val mapReduceAlgorithm = new MapReduceAlgorithm(spark)

    // TODO: implementare un albero per categoria e gestire gli output
    val animalsDF = pivotedDF.withColumn("Context/Topic", when(col("Context/Topic") === "Animals", "Animals").otherwise("Other"))

    mapReduceAlgorithm.dataPreparation(animalsDF)
    mapReduceAlgorithm.generateTree(animalsDF.count().toInt, animalsDF.filter(animalsDF.col("Context/Topic") === "Animals").count().toInt)

    /**

    val seqAlgorithm = new SeqAlgorithm()

    val category = "Animals"

    println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " Starting " + category +
      " tree building...")

    val tree = seqAlgorithm.buildTree(pivotedDF, pivotedDF.columns.filter(_ != "Context/Topic"), category)

    println(tree.toString())
    */
  }
}
