package algorithm

import org.apache.spark.sql.DataFrame
import model.{DecisionNode, LeafNode, Node}
import org.apache.spark.sql.functions.sum

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait AlgorithmUtils {
  /**
   * Computes single attribute's entropy
   *
   * @param catCount Number of attribute's word occurrences in specific category
   * @param totalCount Total number of attribute's word occurrences
   * @param attr Word-as-column we're analyzing
   * @param category Tree's category
   * @return Entropy of an attribute
   * */
  private def calcEntropy(catCount: Double, totalCount: Double, attr: String, category: String): Double = {
    this.entropyFormula(catCount / totalCount) +
      this.entropyFormula(totalCount - catCount / totalCount)
  }

  /**
   *  Computes entropy
   *
   * @param ratio Class probability
   * @return Entropy
   */
  private def entropyFormula(ratio: Double): Double = {
    - (ratio * this.log2(ratio))
  }

  /**
   * Computes attribute's gain ratio
   *
   * @param entropyA Attribute's entropy
   * @param subDFCount DataFrame count after filtered one possible value of attr
   * @param attr Attribute
   * @param totalCount DataFrame total count
   * @return Gain ratio of attribute attr
   */
  private def splitAttribute(entropyA: Double, subDFCount: Double, attr: String, totalCount: Double): Double = {
    var infoA: Double = 0.0
    var splitInfoA: Double = 0.0

    // Computes info and split for every values of attribute attr (0 or > 0)
    Seq(subDFCount, totalCount - subDFCount).foreach { subCount =>
      infoA += (subCount / totalCount) * (
        this.entropyFormula(subCount / totalCount) +
          this.entropyFormula(totalCount - subCount / totalCount)
        )

      splitInfoA += this.entropyFormula(subCount / totalCount)
    }

    // Gain ratio
    (entropyA - infoA) / splitInfoA
  }

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

        val entropyA: Double = this.calcEntropy(categorySum, totalSum, attr, category)

        val attrRatio = this.splitAttribute(entropyA, totalSum, attr, dfCount)

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

  /** Returns the majority classes among dataset */
  private def getMajorityClass(occurMap: Map[String, Seq[Int]], category: String): String = {
    var countCategories: Seq[Int] = Seq(0,0)

    occurMap.foreach( elem => {
      countCategories = (countCategories, elem._2).zipped.map(_ + _)
    })

    if (countCategories.head > countCategories(1)) category else "Other"
  }

  private def log2(num: Double): Double = {
    if (num == 0) 0 else math.log(num) / math.log(2)
  }
}