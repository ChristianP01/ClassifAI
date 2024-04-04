package main.scala.algorithm

import main.scala.model.{DecisionNode, LeafNode, Node}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SeqAlgorithm(spark: SparkSession, maxDepth: Int = 20) {

  /**
   * Recursive methods -> evaluate best attribute and generate link between attributes
   * @param df sub data frame
   * @param dfCount total number of rows
   * @param countCategory number of rows with correct category label
   * @param actualDepth tree depth
   * @param category tree category of interest
   *
   * @return Node - a node class with children
   */
  def generateTree(df: DataFrame, dfCount: Double, countCategory: Double, actualDepth: Int, category: String): Node = {

    /** Entropy of label */
    val entropyGeneral: Double = AlgorithmUtils.calcEntropy(countCategory, dfCount)

    if (actualDepth >= maxDepth || entropyGeneral <= 0.2) {
      return LeafNode(AlgorithmUtils.getMajorLabelByCount(dfCount, countCategory, category))
    }

    /** Sequence of attribute */
    val attributes: Seq[String] = df.drop("Index", "Context/Topic").columns.toSeq

    var maxGainRatio, leftCount, leftCategoryCount = 0.0
    var aBest = ""
    var leftDF = spark.emptyDataFrame

    attributes foreach { attribute =>
      /** Data frame with attribute's value 1 */
      val df1 = df.where(df.col(attribute) === 1)
      /** Data frame count with attribute's value 1 */
      val df1Count = df1.count.toDouble
      /** Data frame category count with attribute's value 1 */
      val df1CategoryCount = df1.where(df1.col("Context/Topic") === category).count.toDouble
      /** Data frame count with attribute's value 0 (complementary of 1) */
      val df0Count = dfCount - df1Count
      /** Data frame category count with attribute's value 0 (complementary of 1) */
      val df0CategoryCount = countCategory - df1CategoryCount

      /** Check if this attribute is significant */
      if (df1Count > 0.0 && df0Count > 0.0) {
        /** Entropy of label in data frame with attribute's value 1 */
        val entropyA1 = AlgorithmUtils.calcEntropy(df1CategoryCount, df1Count)
        /** Entropy of label in data frame with attribute's value 0 */
        val entropyA0 = AlgorithmUtils.calcEntropy(df0CategoryCount, df0Count)

        val info = ((df1Count / dfCount) * entropyA1) + ((df0Count / dfCount) * entropyA0)
        val splitInfo = AlgorithmUtils.entropyFormula(df1Count / dfCount) +
          AlgorithmUtils.entropyFormula(df0Count / dfCount)

        /** Gain ratio of attribute */
        val gainRatio = (entropyGeneral - info) / splitInfo

        if (gainRatio > maxGainRatio) {
          maxGainRatio = gainRatio
          aBest = attribute
          leftDF = df1.drop(aBest)
          leftCount = df1Count
          leftCategoryCount = df1CategoryCount
        }
      }
    }

    val rightDF = df.where(df.col(aBest) === 0).drop(aBest)

    /** Check if one split is empty or aBest is irrelevant */
    if (leftDF.isEmpty || rightDF.isEmpty || maxGainRatio <= 0.0)
      LeafNode(AlgorithmUtils.getMajorLabelByCount(dfCount, countCategory, category))
    else {
      val leftChild = this.generateTree(leftDF, leftCount, leftCategoryCount, actualDepth + 1, category)
      val rightChild = this.generateTree(rightDF, dfCount - leftCount, countCategory - leftCategoryCount,
        actualDepth + 1, category)

      DecisionNode(aBest, leftChild, rightChild)
    }
  }
}
