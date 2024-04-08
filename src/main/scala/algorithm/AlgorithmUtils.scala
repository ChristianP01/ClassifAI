package main.scala.algorithm
import main.scala.model.{DecisionNode, LeafNode, Node}
import org.apache.spark.sql.DataFrame

object AlgorithmUtils extends Serializable {
  def calcEntropy(catCount: Double, totalCount: Double): Double = {
    this.entropyFormula(catCount / totalCount) + this.entropyFormula((totalCount - catCount) / totalCount)
  }

  private def log2(num: Double): Double = {
    if (num == 0) 0 else math.log(num) / math.log(2)
  }

  def entropyFormula(ratio: Double): Double = {
    - (ratio * this.log2(ratio))
  }

  /** Return category with highest count */
  def getMajorLabelByCount(dfCount: Double, countCategory: Double, category: String): String = {
    if (countCategory > dfCount / 2)
      category
    else
      "Other"
  }

  /** Split data frame in training and test */
  def splitDataFrame(df: DataFrame): Map[String, DataFrame] = {
    val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed = 160343280324L)

    Map("Training" -> train, "Test" -> test)
  }

  /** Predict sentence's label given a tree
   *
   * @param root Node - tree root
   * @param sentence Seq[String] - sequence of sentence's words
   * @return String - label
   * */
  private def predict(root: Node, sentence: Seq[String]): String = {
    var tree: Node = root

    while(tree.isInstanceOf[DecisionNode])
      if (sentence.contains(tree.asInstanceOf[DecisionNode].getAttribute))
        tree = tree.asInstanceOf[DecisionNode].getLeft
      else
        tree = tree.asInstanceOf[DecisionNode].getRight

    tree.asInstanceOf[LeafNode].getLabel
  }

  /** Evaluate correct label across all trees
   *
   * @param trees Map[String, Node] - map of tree for each category
   * @param sentence Seq[String] - sequence of sentence's words
   * @param categoryCounts Map[String, Double] - map of count for each category
   * @return String - definitive label
   *  */
  private def evaluateSentence(trees: Map[String, Node], sentence: Seq[String], categoryCounts: Map[String, Double]): String = {
    /** Predictions of all trees */
    val predictions = trees.values.map(predict(_, sentence)).toSeq
    /** Predictions different from Other */
    val nonOtherPredictions = predictions.filter(_ != "Other")

    nonOtherPredictions match {
      /** Every category predicted Other, takes the one with most entries in DF */
      case Nil => categoryCounts.maxBy(_._2)._1

      /** Only one category predicted itself */
      case singleResult :: Nil => singleResult

      /** More than one category predicted itself, takes the one with most entries in DF only between them */
      case _ => categoryCounts.filter { case (category, _) => nonOtherPredictions.contains(category) }.maxBy(_._2)._1
    }
  }

  /** Predict labels for test data frame and evaluate accuracy score
   *
   * @param testDF DataFrame - test data frame
   * @param trees Map[String, Node] - decision trees
   * @param categoryCounts Map[String, Double] - map of count for each category
   */
  def calcMetrics(testDF: DataFrame, trees: Map[String, Node], categoryCounts: Map[String, Double]): Unit = {
    /** Each row with following format:
     * 0 --> Ground truth
     * 1 --> Row ID
     * 2 --> Tokens
     */
    val map = testDF.rdd.map { row =>
      if (this.evaluateSentence(trees, row(2).asInstanceOf[Seq[String]], categoryCounts)
        .equals(row(0).asInstanceOf[String]))
        ("correct", 1.0)
      else
        ("wrong", 1.0)
    }
      .reduceByKey(_ + _)
      .collectAsMap()

    println("Accuracy: " + (map("correct") / (map("correct") + map("wrong"))))
  }
}
