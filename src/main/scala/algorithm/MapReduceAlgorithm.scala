package algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class MapReduceAlgorithm (spark: SparkSession) {
  private val sc = spark.sparkContext
  /** attribute, (row_id, label) */
  private var attrTable: RDD[(String, (Int, String))] = sc.emptyRDD[(String, (Int, String))]
  /** attribute, (label, count) */
  private var countTable: RDD[(String, (String, Int))] = sc.emptyRDD[(String, (String, Int))]
  /** node_id (hash), row_id */
  val hashTable: Seq[(String, Int)] = Seq.empty

  def calcEntropy(catCount: Double, totalCount: Double): Double = {
    this.entropyFormula(catCount / totalCount) +
      this.entropyFormula(totalCount - catCount / totalCount)
  }

  private def log2(num: Double): Double = {
    if (num == 0) 0 else math.log(num) / math.log(2)
  }

  private def entropyFormula(ratio: Double): Double = {
    - (ratio * this.log2(ratio))
  }

  /**
   * Convert the dataframe in a data structure for further MapReduce processing.
   * Executed only one time.
   */
  def dataPreparation(df: DataFrame): Unit = {
    /** Populate attrTable */
   attrTable = df.rdd.flatMap { row =>
      row
        .toSeq
        .zipWithIndex
        .slice(2, row.toSeq.length)
        .filter(_._1 == "1")
        .map {
          case (_, idx) =>
            (row.schema.fieldNames(idx), (row(0).toString.toInt, row(1).toString)) // attribute name, (row_id, label)
        }
    }

    countTable = attrTable.map {
        case (k, (_, l)) => ((k, l), 1)
      }
      .reduceByKey(_ + _)
      .map {
        case ((k, l), count) => (k, (l, count)) // attribute name, (label, count)
      }

    println(countTable.collect().mkString("Array(", ", ", ")"))
  }

  def attributeSelection(): Unit = {
    val totalCounts: RDD[(String, Int)] = this.countTable.map {
      case (k, (_, cnt)) => (k, cnt)
    }
    .reduceByKey(_ + _)

    /** attribute, ((label, countLabel), countAll) */
    val joinedRDD = this.countTable
      .join(totalCounts)
      .map {
        case (k, ((_, cnt), all)) =>
          val infoA = (cnt / all) * this.calcEntropy(cnt, all)
          val splitInfoA = this.entropyFormula(cnt/all)
          (k, (infoA, splitInfoA)) // attribute, (info_attr_l, split_attr_l)
      }
      // .reduceByKey(_)

    println(joinedRDD.collect().mkString("Array(", ", ", ")"))
  }
}
