import data.DataframeCleaner
import algorithm.{AlgorithmUtils, MapReduceAlgorithm, SeqAlgorithm}
import model.Node
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io._
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

    // TODO: sostituire con TopicIndex.getTopicSeq
    val categories: Seq[String] = Seq("Animals")

    /** Map of tree where key is category's name and value is a binary decision tree */
    val trees: mutable.Map[String, Node] = mutable.Map.empty[String, Node]

    if(areTreesNew) {
      categories foreach { category =>
        println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) +
          " Start " + category + " tree generation...")

        val mapReduceAlgorithm = new MapReduceAlgorithm()

        // TODO: implementare un albero per categoria e gestire gli output
        val animalsDF = pivotedDF.withColumn("Context/Topic",
          when(col("Context/Topic") === category, category).otherwise("Other"))

        println(animalsDF.columns.length - 2)

        val tree = mapReduceAlgorithm.generateTree(animalsDF, animalsDF.count().toDouble,
          animalsDF.filter(animalsDF.col("Context/Topic") === category).count().toDouble, 0, category)

        /** Add the category tree to the sequence of trees */
        trees(category) = tree

        println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) +
          " End " + category + " tree generation")

        println(tree.toString(""))

        /** Write binary representation of tree to file */
        val treePath = actualPath + "/trees/" + category + ".bin"
        val out = new ObjectOutputStream(new FileOutputStream(treePath))
        out.writeObject(tree)
        out.close()
      }
    } else {
      println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " Start loading trees...")

      categories foreach { category =>
        /** Read binary representation of tree from file */
        val treePath = actualPath + "/trees/" + category + ".bin"
        val in = new ObjectInputStream(new FileInputStream(treePath))
        val tree: Node = in.readObject().asInstanceOf[Node]
        in.close()

        trees(category) = tree
      }

      println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " End loading trees")
    }
    
    println(AlgorithmUtils.predict(trees("Animals"), "Dog dog dog dog".toLowerCase.split(" ")))
    println(AlgorithmUtils.predict(trees("Animals"), "The bear is sleeping".toLowerCase.split(" ")))
    println(AlgorithmUtils.predict(trees("Animals"), "Scalable cloud computing is great".toLowerCase.split(" ")))
    println(AlgorithmUtils.predict(trees("Animals"), "My dog is not a cat".toLowerCase.split(" ")))

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
