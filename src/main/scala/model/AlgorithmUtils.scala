package model

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait AlgorithmUtils {
  /** Compute the entropy */
  private def entropy(df: DataFrame, classes: List[String]): Double = {
    val S: Long = df.count()
    val numClasses: List[Double] = classes.map(c => df.filter(df("Topic/Context") === c).count().toDouble / S)
    numClasses.map(p => -p * this.log(p)).sum
  }

  /** Compute the gain */
  private def gain(unionSet: DataFrame, classes: List[String], subsets: List[DataFrame]): Double = {
    val S = unionSet.count()
    val impurityBeforeSplit = entropy(unionSet, classes)
    val weights = subsets.map(_.count().toDouble / S)

    val impurityAfterSplit = weights.zip(subsets).map {
      case (w, s) => w * entropy(s, classes)
    }.sum

    impurityBeforeSplit - impurityAfterSplit
  }

  /** Returns the majority classes among dataset */
  private def getMajorityClass(data: DataFrame, classes: List[String]): Int = {
    val numClasses: List[Int] = classes.map(c => data.filter(data("Topic/Context") === c).count().toInt)
    numClasses.max
  }

  /** Check if all instances in the dataset belong to the same class */
  private def allSameClass(data: DataFrame): Boolean = {
    data.select("Topic/Context").distinct().count() == 1
  }

  /** Build the decision tree */
  def buildTree(data: DataFrame, attributes: List[String], classes: List[String]): Node = {
    if (allSameClass(data)) {
      LeafNode(data.select("Topic/Context").first().getString(0))
    } else if (attributes.isEmpty) {
      LeafNode(classes(getMajorityClass(data, classes)))
    } else {
      val gains = attributes.map { attr =>
        val groupedData = data.groupBy(attr).count()
        val subsetGains = groupedData.collect().map { row =>
          val attrValue = row.getString(0)
          val subset = data.filter(col(attr) === attrValue)
          gain(data, classes, List(subset))
        }
        (attr, subsetGains.sum)
      }

      val bestAttribute = gains.maxBy(_._2)._1
      val remainingAttributes = attributes.filterNot(_ == bestAttribute)
      val children = data.select(bestAttribute).distinct().collect().map(row =>
        buildTree(data.filter(data(bestAttribute) === row.get(0)), remainingAttributes, classes)).toList
      DecisionNode(bestAttribute, None, children)
    }
  }

  private def log(num: Double): Double = {
    if (num == 0) {
      0
    } else {
      math.log(num)/math.log(2)
    }
  }
}