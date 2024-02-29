import data.DataframeCleaner
import algorithm.SeqAlgorithm
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Main {
  def main(args: Array[String]): Unit = {
    // Start Spark session
    val spark = SparkSession
      .builder
      .appName("ClassifAI")
      .master("local[*]")
      .getOrCreate()

    // Upload dataframe
    val originalDF = spark.read.option("header", value = true).csv(System.getProperty("user.dir") +
      "/src/main/assets/Context.csv")

    // Preprocess dataframe
    val preprocessor = new DataframeCleaner(spark, originalDF)

    // Saving preprocessed and pivoted df to apply spark transformation and optimize execution time
    preprocessor.saveDataFrame(preprocessor.getPivotedDataFrame)

    val occurMap = preprocessor.getOccurrenceMap

    // Read preprocessed dataframe
    val pivotedDF = spark.read.option("header", value = true).csv(System.getProperty("user.dir") + "/output/")

    println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " Starting tree building...")

    val seqAlgorithm = new SeqAlgorithm()

    val tree = seqAlgorithm.buildTree(pivotedDF, occurMap, occurMap.keySet.toSeq, "Animals")

    println(tree.toString)
  }
}