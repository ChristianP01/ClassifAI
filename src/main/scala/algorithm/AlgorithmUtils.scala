package algorithm
import model.{DecisionNode, LeafNode, Node}

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

  def predict(root: Node, sentence: Seq[String]): String = {
    var tree: Node = root

    while(tree.isInstanceOf[DecisionNode])
      if (sentence.contains(tree.asInstanceOf[DecisionNode].getAttribute))
        tree = tree.asInstanceOf[DecisionNode].getLeft
      else
        tree = tree.asInstanceOf[DecisionNode].getRight

    tree.asInstanceOf[LeafNode].getLabel
  }
}
