import data.{DataframeCleaner, TopicIndex}
import algorithm.{AlgorithmUtils, MapReduceAlgorithm}
import model.Node
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.SparkSession

import java.io._
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

object Main {
  def main(args: Array[String]): Unit = {
    /**
     * Change isDFNew to execute or skip data frame preprocessing and saving.
     *
     * true -> preprocess and save a new dataset
     * false -> load a previous preprocessed dataset
     */
    val isDFNew: Boolean = false

    /**
     * Change areTreesNew to execute or skip tree generations and saving.
     *
     * true -> generates new trees and save them
     * false -> load previous generated trees
     */
    val areTreesNew: Boolean = false

    val actualPath = System.getProperty("user.dir")

    /** Start Spark session */
    val spark = SparkSession
      .builder
      .appName("ClassifAI")
      .master("local[*]")
      .getOrCreate()

    if (isDFNew) {
      /** Upload dataframe */
      val originalDF = spark.read.option("header", value = true).csv(actualPath + "/src/main/assets/Context.csv")

      /** Preprocess dataframe */
      val preprocessor = new DataframeCleaner(spark, originalDF)

      /** Saving preprocessed and pivoted df to apply spark transformation and optimize execution time */
      preprocessor.saveDataFrame(preprocessor.getPivotedDataFrame, actualPath + "/dfOutput/")
    }

    /** Read preprocessed dataframe */
    val pivotedDF = spark.read.option("header", value = true).csv(actualPath + "/dfOutput/")

    val categories: Seq[String] = TopicIndex.getTopicSeq

    /** Map of tree where key is category's name and value is a binary decision tree [category name, tree] */
    val trees: mutable.Map[String, Node] = mutable.Map.empty[String, Node]

    val dfCount = pivotedDF.count().toDouble

    /** Map each category to its count [category name, count] */
    val categoryCounts = categories.map { category =>
      (category, pivotedDF.filter(pivotedDF.col("Context/Topic") === category).count().toDouble)
    }.toMap

     if(areTreesNew) {
      categories foreach { category =>
        println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) +
          " Start " + category + " tree generation...")

        val mapReduceAlgorithm = new MapReduceAlgorithm()

        /** Change dataframe to binary label -> non-category = Other */
        val categoryDF = pivotedDF.withColumn("Context/Topic",
          when(col("Context/Topic") === category, category).otherwise("Other"))

        val tree = mapReduceAlgorithm.generateTree(categoryDF, dfCount, categoryCounts(category), 0, category)

        /** Add the category tree to the sequence of trees */
        trees(category) = tree

        println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) +
          " End " + category + " tree generation")

        /** Write binary representation of the tree to file */
        val treePath = actualPath + "/trees/"
        if (!Files.exists(Paths.get(treePath))) Files.createDirectories(Paths.get(treePath))
        val out = new ObjectOutputStream(new FileOutputStream(treePath + category + ".bin"))
        out.writeObject(tree)
        out.close()
      }
    } else {
      println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " Start loading trees...")

      categories foreach { category =>
        /** Read binary representation of the tree from file */
        val treePath = actualPath + "/trees/" + category + ".bin"
        val in = new ObjectInputStream(new FileInputStream(treePath))
        val tree: Node = in.readObject().asInstanceOf[Node]
        in.close()

        trees(category) = tree
      }

      println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " End loading trees")
    }

    println(AlgorithmUtils.evaluateSentence(trees.toMap, "Dog dog dog dog".toLowerCase.split(" "), categoryCounts))
    println(AlgorithmUtils.evaluateSentence(trees.toMap, "The bear is sleeping".toLowerCase.split(" "), categoryCounts))
    println(AlgorithmUtils.evaluateSentence(trees.toMap, "Scalable cloud computing is great".toLowerCase.split(" "),
      categoryCounts))
    println(AlgorithmUtils.evaluateSentence(trees.toMap, "My dog is not a cat".toLowerCase.split(" "), categoryCounts))
    println(AlgorithmUtils.evaluateSentence(trees.toMap, "aaaaaaaaaaaa".toLowerCase.split(" "), categoryCounts))

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
