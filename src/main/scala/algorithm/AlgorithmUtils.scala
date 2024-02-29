package algorithm

import data.TopicIndex
import org.apache.spark.sql.DataFrame
import model.{DecisionNode, LeafNode, Node}
import org.apache.spark.sql.functions.desc

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait AlgorithmUtils {
  /** Calculate single attribute's entropy
   *
   * @param occurMap Map structured as [word, list of occurrences in every category]
   * @param attr Word-as-column we're analyzing
   * @param category Tree's category
   * */
  private def calcEntropy(occurMap: Map[String, Seq[Int]], attr: String, category: String): Double = {

    // Occurrences of attr (word) in all dataset
    val occurs: Seq[Int] = occurMap(attr)
    val occursSum = occurs.sum

    // Occurrences of attr (word) of a specific category
    val catOccurs: Int = occurs(TopicIndex.getIndex(category))

    this.entropyFormula(catOccurs.toDouble / occursSum) +
    this.entropyFormula(occurs.length-catOccurs / occursSum)
  }

  private def entropyFormula(x: Double): Double = {
    -(x * this.log2(x))
  }

  private def splitAttributes(entropyA: Double, dfCount: Long, attr: String, totalCount: Int): (Double, String) = {
    var infoA: Double = 0.0
    var splitInfoA: Double = 0.0

    infoA += (dfCount / totalCount) * (
      this.entropyFormula(dfCount.toDouble / totalCount) +
      this.entropyFormula((totalCount - dfCount).toDouble / totalCount)
    )

    splitInfoA += this.entropyFormula(dfCount.toInt / totalCount)

    ((entropyA - infoA) / splitInfoA) -> attr
  }

  /** Build the decision tree */
  def buildTree(df: DataFrame, occurMap: Map[String, Seq[Int]], attributes: List[String], category: String): Node = {

    val dfCount = df.count().toInt

    // return Tree as a single node with most frequent class
     if (attributes.isEmpty || dfCount == 0) {
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

      gainRatios += this.splitAttributes(entropyA, subDFCount, attr, dfCount)
      gainRatios += this.splitAttributes(entropyA, dfCount-subDFCount, attr, dfCount)
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


    if (countCategory > (dfCount-countCategory)) {
      category
    } else {
      "Other"
    }
  }

  private def log2(num: Double): Double = {
    if (num == 0) {
      0
    } else {
      math.log(num)/math.log(2)
    }
  }
}