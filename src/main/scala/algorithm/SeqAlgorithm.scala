package algorithm

import model.{DecisionNode, LeafNode, Node}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class SeqAlgorithm extends IAlgorithm {
/**
  /**
   * Generates the tree
   *
   * @param df DataFrame
   * @param attributes Sequence of attributes
   * @param category Label of the tree
   * @return Node
   */
  def buildTree(df: DataFrame, attributes: Seq[String], category: String): Node = {
    val dfCount = df.count().toDouble

    if (dfCount == 0) {
      println("dfCount 0")
      // TODO: return qualcosa, failure?
      return null
    }

    // return Tree as a single node with most frequent class
    if (attributes.isEmpty) {
      // TODO: sistemare majority class
      //return LeafNode(this.getMajorityClass(category))
    }

    // return tree as a single node
    if (attributes.length == 1) {
      println("attributes == 1")
      return LeafNode(attributes.head) // TODO: non deve tornare l'attributo ma una classe
    }

    var maxGainRatio = 0.0
    var aBest: String = ""

    // TODO: Verificare che esistano ancora righe con category
    attributes.foreach(attr => {
      val newDF = df.select("Context/Topic", attr).groupBy("Context/Topic").agg(sum(attr))

      val filteredDF = newDF.where(newDF.col("Context/Topic") === category)

      var categorySum: Double = 0.0

      if (!filteredDF.isEmpty) {
        categorySum = filteredDF.first().getDouble(1)

        val totalSum: Double = newDF.select(sum("sum(" + attr + ")")).first().getDouble(0)

        val entropyA: Double = calcEntropy(categorySum, totalSum, attr, category)

        val attrRatio = splitAttribute(entropyA, totalSum, attr, dfCount)

        if (attrRatio > maxGainRatio) {
          maxGainRatio = attrRatio
          aBest = attr
        }
      }
    })

    val actualNode: DecisionNode = DecisionNode(aBest, List())

    println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " Attributes: " + attributes.length)
    println(aBest)
    println(maxGainRatio)

    actualNode.addNode(this.buildTree(df.where(df.col(aBest) === 0).drop(aBest), attributes.filter(_ != aBest), category))
    actualNode.addNode(this.buildTree(df.where(df.col(aBest) > 0).drop(aBest), attributes.filter(_ != aBest), category))

    actualNode
  }
*/
}
