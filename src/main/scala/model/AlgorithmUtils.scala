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

  /** Split nodes based on the information gains returned from each dataset's attribute  */
  protected def findBestSplit(data: DataFrame, classes: List[String]): Unit = {

    //    Determine the dataset’s overall entropy: This gives the impurity in the data a baseline measurement.
    // classMap contains entries like [className, classEntropy]
    val (overallEntropy, classMap): (Double, Map[String, Double]) = this.calcEntropy(data, classes)

    //    Determine the entropy of each division for each attribute: Calculate the entropy of each partition that results
        //    from splitting the dataset according to the attribute’s potential values.


    //      Calculate the information gain for each attribute: Take the average entropy of each attribute’s divisions
        //      and deduct it from the dataset’s starting entropy.
        //      This figure shows how much less entropy was produced by dividing the data according to that characteristic.
    

    //      Select the feature that yields the most information gain: The decision tree’s current node has chosen
        //      to split this property since it is thought to be the most informative.


    //      For every resultant partition, repeat the following steps: Apply the same procedure recursively to the partitions
        //      that the split produced, choosing the most informative feature
        //      for each division and building the decision tree top-down.

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