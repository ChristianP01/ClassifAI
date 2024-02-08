package model

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait AlgorithmUtils {
  /** Compute the entropy */
  protected def calcEntropy(df: DataFrame, classes: List[String]): (Double, Map[String, Double]) = {
    val totalCount: Long = df.count()
    var classMap: Map[String, Double] = Map.empty

    classes.foreach( c =>
      classMap += c -> df.filter(df("Topic/Context") === c).count().toDouble
    )

    // Return the total entropy and a map structured as [classType, classEntropy]
    (classMap.map( c =>
      -(c._2 / totalCount) * this.log2(c._2 / totalCount)
    ).sum, classMap)
  }

  /** Compute the gain */
  protected def gain(df: DataFrame, classes: List[String], subsets: List[DataFrame]): Double = {
    val totalCount = df.count()
    val impurityBeforeSplit = this.calcEntropy(df, classes)._1
    val weights: List[Double] = subsets.map(_.count() / totalCount)

    val impurityAfterSplit: Double = weights.zip(subsets).map { obj =>
      obj._1 * this.calcEntropy(obj._2, classes)._1
    }.sum

    impurityBeforeSplit - impurityAfterSplit
  }

  /** Returns the majority classes among dataset */
  protected def getMajorityClass(data: DataFrame, classes: List[String]): Int = {
    val numClasses: List[Int] = classes.map(c => data.filter(data("Topic/Context") === c).count().toInt)
    numClasses.max
  }

  /** Check if all instances in the dataset belong to the same class */
  protected def allSameClass(data: DataFrame): Boolean = {
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

  protected def log2(num: Double): Double = {
    if (num == 0) {
      0
    } else {
      math.log(num)/math.log(2)
    }
  }
}