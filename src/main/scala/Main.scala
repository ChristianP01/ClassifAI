package main.scala

import algorithm.{AlgorithmUtils, MapReduceAlgorithm}
import data.{DataframeCleaner, TopicIndex}
import model.Node
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import java.io._
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

object Main {
  def main(args: Array[String]): Unit = {
    println("Received arguments: " + args.mkString("Array(", ", ", ")"))

    /**
     * Retrieve (key, value) pair for each given argument and put in a map
     * */
    val args_map: Map[String, String] = args.map { arg =>
      val argSplit = arg.split("=", 2)
      (argSplit(0), argSplit(1))
    }.toMap

    /**
     * If user provides a path, data (and consequently computation) will be based on cloud resources,
     * else it will be local
     * */
    val actualPath: String = args_map.getOrElse("actualPath", System.getProperty("user.dir") + "/src/main/assets")

    /**
     * Set the minimum occurrences a word must have in the dataset to being used as an attribute
     * */
    val minWordOccurrences: Int = args_map.getOrElse("minOccurs", 200).toString.toInt

    /**
     * true -> preprocess and save a new dataset
     * false -> load a previous preprocessed dataset
     * Defaults to true
     * */
    val computeDF: Boolean = args_map.getOrElse("computeDF", true).toString.toBoolean

    /**
     * true -> generates new trees and save them
     * false -> load previous generated trees
     * Defaults to true
     * */
    val computeTrees: Boolean = args_map.getOrElse("computeTrees", true).toString.toBoolean

    /** Start Spark session */
    val spark = SparkSession
      .builder
      .appName("ClassifAI")
      .getOrCreate()

    if (computeDF) {
      /** Upload dataframe */
      val originalDF = spark.read.option("header", value = true).csv(actualPath + "/Context.csv")

      /** Preprocess dataframe */
      val preprocessor = new DataframeCleaner(spark, originalDF, minWordOccurrences)

      /** Saving preprocessed and pivoted df to apply spark transformation and optimize execution time */
      preprocessor.saveDataFrame(preprocessor.getPivotedDataFrame, actualPath + "/dfOutput/")
    }

    /** Read preprocessed dataframe */
    var pivotedDF = spark.read.option("header", value = true).csv(actualPath + "/dfOutput/")

    pivotedDF = pivotedDF.repartition(96).cache()

    println("Number of words: " + (pivotedDF.columns.length - 2))

    val categories: Seq[String] = TopicIndex.getTopicSeq

    /** Map of tree where key is category's name and value is a binary decision tree [category name, tree] */
    val trees: mutable.Map[String, Node] = mutable.Map.empty[String, Node]

    val dfCount = pivotedDF.count().toDouble

    /** Map each category to its count [category name, count] */
    val categoryCounts = categories.map { category =>
      (category, pivotedDF.filter(pivotedDF.col("Context/Topic") === category).count().toDouble)
    }.toMap

     if(computeTrees) {
      categories foreach { category =>
        println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) +
          " Start " + category + " tree generation...")

        val mapReduceAlgorithm = new MapReduceAlgorithm()

        /** Change dataframe to binary label -> non-category = Other */
        var categoryDF = pivotedDF.withColumn("Context/Topic",
          when(col("Context/Topic") === category, category).otherwise("Other"))

        categoryDF = categoryDF.repartition(96).cache()

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

    println(AlgorithmUtils.evaluateSentence(trees.toMap, "Human body is a perfect machine.".toLowerCase.split(" "),
      categoryCounts))
    println(AlgorithmUtils.evaluateSentence(trees.toMap, "The dog is sleeping near the campfire".toLowerCase.split(" "),
      categoryCounts))
    println(AlgorithmUtils.evaluateSentence(trees.toMap, "Scalable cloud computing is great".toLowerCase.split(" "),
      categoryCounts))
    println(AlgorithmUtils.evaluateSentence(trees.toMap, "I really love you so much".toLowerCase.split(" "),
      categoryCounts))
    println(AlgorithmUtils.evaluateSentence(trees.toMap, "God bless you!".toLowerCase.split(" "), categoryCounts))

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
