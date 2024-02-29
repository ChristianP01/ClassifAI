import data.{DataframeCleaner, TopicIndex}
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

    /** Upload dataframe */
    val originalDF = spark.read.option("header", value = true).csv(System.getProperty("user.dir") +
      "/src/main/assets/Context.csv")

    /** Preprocess dataframe */
    val preprocessor = new DataframeCleaner(spark, originalDF)

    /** Saving preprocessed and pivoted df to apply spark transformation and optimize execution time */
    preprocessor.saveDataFrame(preprocessor.getPivotedDataFrame)

    /** Generate a map with categories count for each word */
    val occurMap = preprocessor.getOccurrenceMap

    /** Read preprocessed dataframe */
    val pivotedDF = spark.read.option("header", value = true).csv(System.getProperty("user.dir") + "/output/")

    val seqAlgorithm = new SeqAlgorithm()

    val category = "Animals"

    // TODO: implementare un albero per categoria e gestire gli output
    println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " Starting " + category +
      " tree building...")

    /** Aggregate count of categories different from the one of the tree */
    val categoryMap = occurMap.mapValues { seq =>
      val categoryCount = seq(TopicIndex.getIndex(category))
      val otherSum = seq.sum - categoryCount

      Seq(categoryCount, otherSum)
    }

    val tree = seqAlgorithm.buildTree(pivotedDF, categoryMap, occurMap.keySet.toSeq, category)

    println(tree.toString)
  }
}