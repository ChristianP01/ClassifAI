package main.scala.algorithm
import main.scala.model.{DecisionNode, LeafNode, Node}

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
  def evaluateSentence(trees: Map[String, Node], sentence: Seq[String], categoryCounts: Map[String, Double]): String = {
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
}
