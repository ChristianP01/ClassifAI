package algorithm

import data.TopicIndex
import org.apache.spark.sql.DataFrame
import model.{DecisionNode, LeafNode, Node}

trait AlgorithmUtils {
  /** Compute the entropy */
  private def calcEntropy(df: DataFrame, classes: List[String]): (Double, Map[String, Double]) = {
    val totalCount: Long = df.count()
    var classMap: Map[String, Double] = Map.empty

    classes.foreach( c =>
      classMap += c -> df.filter(df("Context/Topic") === c).count().toDouble
    )

    // Return the total entropy and a map structured as [classType, classEntropy]
    (classMap.map( c =>
      -(c._2 / totalCount) * this.log2(c._2 / totalCount)
    ).sum, classMap)
  }

  private def entropyFormula(x: Double, total: Integer): Double = {
    -((x/total) * this.log2(x/total))
  }

  /** Calculate single attribute's entropy
   *
   * @param occurMap Map structured as [word, list of occurrence in every category]
   * @param attr Word-as-column we're analyzing
   * @param category Tree's category
   * */
  private def calcEntropy(occurMap: Map[String, List[Integer]], attr: String, category: String): Double = {

    // Occurrences of attr (word) in all dataset
    val occurs: List[Integer] = occurMap(attr)

    // Occurrences of attr (word) of a specific category
    val catOccurs: Integer = occurs(TopicIndex.getIndex(category))

    this.entropyFormula(catOccurs.toDouble, occurs.length) +
    this.entropyFormula(occurs.length-catOccurs, occurs.length)
  }

  /** Compute the gain
   * */
  private def gain(df: DataFrame, classes: List[String], subsets: List[DataFrame]): Double = {
    val totalCount = df.count()
    val impurityBeforeSplit = this.calcEntropy(df, classes)._1
    val weights: List[Double] = subsets.map(_.count().toDouble / totalCount.toDouble)

    val impurityAfterSplit: Double = weights.zip(subsets).map { obj =>
      obj._1 * this.calcEntropy(obj._2, classes)._1
    }.sum

    impurityBeforeSplit - impurityAfterSplit
  }

  /** Returns the majority classes among dataset */
  private def getMajorityClass(data: DataFrame, classes: List[String]): Int = {
    val numClasses: List[Int] = classes.map(c => data.filter(data("Context/Topic") === c).count().toInt)
    numClasses.max
  }

  /** Check if all instances in the dataset belong to the same class */
  private def allSameClass(data: DataFrame): Boolean = {
    data.select("Context/Topic").distinct().count() == 1
  }

  /** Split nodes based on the information gains returned from each dataset's attribute  */
  protected def findBestSplit(data: DataFrame, classes: List[String]): Unit = {

    //    Determine the dataset’s overall entropy: This gives the impurity in the data a baseline measurement.
    // classMap contains entries like [className, classEntropy]
    val (overallEntropy, classEntropyMap): (Double, Map[String, Double]) = this.calcEntropy(data, classes)

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
  private def buildTree(df: DataFrame, occurMap: Map[String, List[Integer]], attributes: List[String],
                        category: String): Node = {

    // return failure
//    if (df.count() == 0)


    // return Tree as a single node with most frequent class
     if (attributes.isEmpty)
       LeafNode("")

    // return tree as a single node
    if (attributes.length == 1)
      LeafNode("") // TODO: what label?

    val tree: DecisionNode = DecisionNode("", List())
    var gainRatios: Map[Double, String] = Map.empty

    attributes.foreach(attr => {
      var infoGainA: Double = 0.0
      var splitInfoA: Double = 0.0

      val entropyA: Double = this.calcEntropy(occurMap, attr, category)

      List[Integer](0, 1).foreach(i => {
        val valuesA = df.where(df.col(attr) === i)

        val dfRatio: Double = valuesA.count() / df.count()

        infoGainA += dfRatio * (
            this.entropyFormula(valuesA.count().toDouble, df.count().toInt) +
            this.entropyFormula((df.count() - valuesA.count()).toDouble, df.count().toInt)
          )

        splitInfoA += this.entropyFormula(dfRatio, df.count().toInt)
      })

      gainRatios += ((entropyA - infoGainA) / splitInfoA) -> attr
    })

    // Return attribute having argmax(gainRatio)
    val aBest: String = gainRatios(gainRatios.keySet.max)
    tree.addNode(DecisionNode(aBest, List()))

    List[Integer](0, 1).foreach(i => {
      this.buildTree(df.where(df.col(aBest) === i), occurMap, attributes, category)
    })

    tree
  }

  private def log2(num: Double): Double = {
    if (num == 0) {
      0
    } else {
      math.log(num)/math.log(2)
    }
  }
}