package algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    this.entropyFormula((totalCount - catCount) / totalCount)
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

    // MAP_ATTRIBUTE: populate attrTable
   attrTable = df.rdd.flatMap { row =>
      row
        .toSeq
        .zipWithIndex
        .slice(2, row.toSeq.length)
//        .filter(_._1 == "1")
        .map {
          case (_, idx) =>
            (row.schema.fieldNames(idx), (row(0).toString.toInt, row(1).toString)) // attribute name, (row_id, label)
        }
   }

   // REDUCE_ATTRIBUTE: emit countTable
   countTable = attrTable.map {
       case (k, (_, l)) => ((k, l), 1)
     }
     .reduceByKey(_ + _)
     .map {
       case ((k, l), count) => (k, (l, count)) // attribute name, (label, count)
     }

   println(countTable.collect().mkString("Array(", ", ", ")"))
   }

  /**
   * Recursive methods -> evaluate best attribute and generate link between attributes
   * @param dfCount total number of rows
   * @param cntL number of rows with correct category label
   * */
  def generateTree(dfCount: Int, cntL: Int): Unit = {

    // REDUCE_POPULATION: emit attribute total count
    /** attribute, totalCount */
    val totalCounts: RDD[(String, Int)] = this.countTable.map {
      case (k, (_, cnt)) => (k, cnt)
    }
    .reduceByKey(_ + _)

    // MAP_COMPUTATION
    /** attribute, ((label, countLabel), totalCount) */
    val joinedRDD = this.countTable
      .join(totalCounts)
      .map {
        case (k, ((_, cnt), all)) =>


          val entropyA = this.calcEntropy(cntL, dfCount)

          /** Gain computation */
          val entropyA1 = this.calcEntropy(cnt, all)
          val entropyA0 = this.calcEntropy(cntL - cnt, dfCount - all)
          val infoA = (entropyA1 * (all / dfCount)) + (entropyA0 * ((dfCount - all) / dfCount))
          val splitInfoA = this.entropyFormula(all / dfCount) + this.entropyFormula((dfCount - all) / dfCount)


          (k, (infoA, splitInfoA)) // attribute, (info(v), splitInfo(v))
      }
//      .reduceByKey(_ + _)
    //val gainA = entropyA - infoA
    //val gainRatioA = gainA / splitInfoA

    println(joinedRDD.collect().mkString("Array(", ", ", ")"))
  }
}
