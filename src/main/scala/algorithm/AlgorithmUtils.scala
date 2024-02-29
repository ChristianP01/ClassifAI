package algorithm

import data.TopicIndex
import org.apache.spark.sql.DataFrame
import model.{DecisionNode, LeafNode, Node}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait AlgorithmUtils {
  /**
   * Computes single attribute's entropy
   *
   * @param occurMap Map structured as [word, list of occurrences in every category]
   * @param attr Word-as-column we're analyzing
   * @param category Tree's category
   * @return Entropy of an attribute
   * */
  private def calcEntropy(occurMap: Map[String, Seq[Int]], attr: String, category: String): Double = {
    // Occurrences of attr (word) in all dataset
    val occurs: Seq[Int] = occurMap(attr)
    val occursSum = occurs.sum

    // Occurrences of attr (word) of a specific category
    val catOccurs: Int = occurs(TopicIndex.getIndex(category))

    this.entropyFormula(catOccurs.toDouble / occursSum) +
      this.entropyFormula(occurs.length - catOccurs / occursSum) // TODO: occurs.length è il numero di categorie, differenza con occorrenze di categoria?
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
   * @return Couple of value (gain ratio, attribute)
   */
  private def splitAttribute(entropyA: Double, subDFCount: Int, attr: String, totalCount: Int): (Double, String) = {
    var infoA: Double = 0.0
    var splitInfoA: Double = 0.0

    // Computes info and split for every values of attribute attr (0 or > 0)
    Seq(subDFCount, totalCount - subDFCount).foreach { subCount =>
      infoA += (subCount / totalCount) * (
        this.entropyFormula(subCount.toDouble / totalCount) +
          this.entropyFormula((totalCount - subCount).toDouble / totalCount)
        )

      splitInfoA += this.entropyFormula(subCount / totalCount)
    }

    // Gain ratio
    ((entropyA - infoA) / splitInfoA) -> attr
  }


  /**
   * Generates the tree
   *
   * @param df DataFrame
   * @param occurMap Map structured as [word, list of occurrences in every category]
   * @param attributes Sequence of attributes
   * @param category Label of the tree
   * @return Node
   */
  def buildTree(df: DataFrame, occurMap: Map[String, Seq[Int]], attributes: Seq[String], category: String): Node = {
    val dfCount = df.count().toInt

    if (dfCount == 0) {
      // TODO: return qualcosa, failure?
      return null
    }

    // return Tree as a single node with most frequent class
    if (attributes.isEmpty) {
      return LeafNode(this.getMajorityClass(df, dfCount, category))
    }

    // return tree as a single node
    if (attributes.length == 1) {
      return LeafNode(attributes.head) // TODO: non deve tornare l'attributo ma una classe
    }

    var gainRatios: Map[Double, String] = Map.empty

    /**
    occurMap.foreach(pair => {println(pair._1, pair._2)})
    println(dfCount)
    */

    attributes.foreach(attr => {
      val entropyA: Double = this.calcEntropy(occurMap, attr, category)

      val subDFCount = df.where(df.col(attr) === 0).count().toInt

      gainRatios += this.splitAttribute(entropyA, subDFCount, attr, dfCount)
    })

    // Return attribute having argmax(gainRatio)
    val aBest: String = gainRatios(gainRatios.keySet.max)

    val actualNode: DecisionNode = DecisionNode(aBest, List())

    /**
    println(DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()) + " Attributes: " + attributes.length)
    println(aBest)
*/

    actualNode.addNode(this.buildTree(df.where(df.col(aBest) === 0).drop(aBest), occurMap.filterKeys(_ != aBest),
      attributes.filter(_ != aBest), category))
    actualNode.addNode(this.buildTree(df.where(df.col(aBest) > 0).drop(aBest), occurMap.filterKeys(_ != aBest),
      attributes.filter(_ != aBest), category))

    actualNode
  }

  /** Returns the majority classes among dataset */
  private def getMajorityClass(df: DataFrame, dfCount: Int, category: String): String = {
    val countCategory = df.filter(df.col("Context/Topic") === category).count().toInt

    if (countCategory > (dfCount - countCategory)) category else "Other"
  }

  private def log2(num: Double): Double = {
    if (num == 0) 0 else math.log(num) / math.log(2)
  }
}