package algorithm

import model.{DecisionNode, LeafNode, Node}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class MapReduceAlgorithm {

  private val maxDepth = 10

  /**
   * Recursive methods -> evaluate best attribute and generate link between attributes
   * @param df sub data frame
   * @param dfCount total number of rows
   * @param countCategory number of rows with correct category label
   * @param actualDepth tree depth
   * @param category tree category of interest
   *
   * @return Node - a node class with children
   * */
  def generateTree(df: DataFrame, dfCount: Double, countCategory: Double,
                   actualDepth: Int, category: String, parentAttr: String = ""): Node = {

    /** Entropy of label */
    val entropyGeneral: Double = AlgorithmUtils.calcEntropy(countCategory, dfCount)

    if (actualDepth >= maxDepth) {
      // Return category with highest count
      if (countCategory > dfCount / 2)
        return LeafNode(category)
      else
        return LeafNode("Other")
    }

    /** (attribute, value), (row_id, label) */
    val attrTable: RDD[((String, Double), (Double, String))] = df.rdd.flatMap { row =>
      row
        .toSeq
        .zipWithIndex
        .slice(2, row.toSeq.length)
        .map {
          case (value, idx) =>
            // (attribute name, value), (row_id, label)
            ((row.schema.fieldNames(idx), value.toString.toDouble), (row(0).toString.toDouble, row(1).toString))
        }
    }

    /** (attribute, value), (label, count) */
    val countTable: RDD[((String, Double), (String, Double))] = attrTable.map {
        case ((k, v), (_, l)) => (((k, v), l), 1)
      }
      .reduceByKey(_ + _)
      .map {
        case (((k,v), l), count) => ((k, v), (l, count)) // (attribute name, value), (label, count)
      }

    /** (attribute, value), totalCount */
    val totalCounts: RDD[((String, Double), Double)] = countTable.map {
      case ((k,v), (_, cnt)) => ((k, v), cnt)
    }
    .reduceByKey(_ + _)

    /** (attribute, value), ((label, count), totalCount) */
    val joinRDD: RDD[((String, Double), ((String, Double), Double))] = countTable
      .join(totalCounts)
      .filter(_._2._1._1 == "Other") // Filtering entries with "Other" label, complementary computation in aBest

    /** Best attribute */
    val aBest: String = joinRDD
      .map {
        case ((k,_), ((_, cnt), all)) =>
          /** Gain computation */
          // "all" refers to all entries in which k has a given value, dfCount is total dataset len
          val entropyAv = AlgorithmUtils.calcEntropy(cnt, all)
          val infoAv = entropyAv * (all / dfCount)
          val splitInfoAv = AlgorithmUtils.entropyFormula(all / dfCount)

          (k, (infoAv, splitInfoAv)) // attribute, (info(v), splitInfo(v))
      }
      .reduceByKey((A0, A1) => {
        ((entropyGeneral - (A0._1 + A1._1)) / (A0._2 + A1._2), 0.0)
      })
      .map {
        case (k, (gainRatio, _)) =>
          (k, gainRatio)
      }
      .reduce((k1, k2) => if (k1._2 > k2._2) k1 else k2)._1 // Take the attribute with higher gain ratio

    println(aBest)

    /** Checks if aBest is the same of its parent */
    if (aBest.equals(parentAttr)) {
      if (countCategory > dfCount / 2)
        return LeafNode(category)
      else
        return LeafNode("Other")
    }

    /** ((label, count), totalCount) split with aBest */
    val leftCounts = joinRDD.filter(_._1 == (aBest, 1.0)).first()._2
    val rightCounts = joinRDD.filter(_._1 == (aBest, 0.0)).first()._2

    val leftChild = this.generateTree(df.where(df.col(aBest) === 1), dfCount = leftCounts._2,
      countCategory = leftCounts._2 - leftCounts._1._2, actualDepth + 1, category, aBest)
    val rightChild = this.generateTree(df.where(df.col(aBest) === 0), dfCount = rightCounts._2,
      countCategory = rightCounts._2 - rightCounts._1._2, actualDepth + 1, category, aBest)

    val node = DecisionNode(aBest, leftChild, rightChild)

    node
  }
}