package algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class MapReduceAlgorithm (spark: SparkSession) {
  private val sc = spark.sparkContext
  /** (attribute, value), (row_id, label) */
  private var attrTable: RDD[((String, Double), (Double, String))] = sc.emptyRDD[((String, Double), (Double, String))]
  /** (attribute, value), (label, count) */
  private var countTable: RDD[((String, Double), (String, Double))] = sc.emptyRDD[((String, Double), (String, Double))]
  /** node_id (hash), row_id */
  val hashTable: Seq[(String, Double)] = Seq.empty

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
        .map {
          case (value, idx) =>
            ((row.schema.fieldNames(idx), value.toString.toDouble), (row(0).toString.toDouble, row(1).toString)) // (attribute name, value), (row_id, label)
        }
   }

   // REDUCE_ATTRIBUTE: emit countTable
   countTable = attrTable.map {
       case ((k, v), (_, l)) => (((k, v), l), 1)
     }
     .reduceByKey(_ + _)
     .map {
       case (((k,v), l), count) => ((k, v), (l, count)) // (attribute name, value), (label, count)
     }
   }

  /**
   * Recursive methods -> evaluate best attribute and generate link between attributes
   * @param dfCount total number of rows
   * @param cntL number of rows with correct category label
   * */
  def generateTree(dfCount: Double, cntL: Double): Unit = {

    /** Entropy of label L */
    val entropyGeneral: Double = AlgorithmUtils.calcEntropy(cntL, dfCount)

    // REDUCE_POPULATION: emit attribute total count
    /** (attribute, value), totalCount */
    val totalCounts: RDD[((String, Double), Double)] = this.countTable.map {
      case ((k,v), (_, cnt)) => ((k, v), cnt)
    }
    .reduceByKey(_ + _)

    // MAP_COMPUTATION
    /** Best attribute */
    val aBest: String = this.countTable
      .join(totalCounts)
      .filter(_._2._1._1 == "Other") // Filtering entries with "Other" label
      .map {
        case ((k,_), ((_, cnt), all)) =>
          /** Gain computation */
          // "all" refers to all entries in which k has a given value, dfCount is total dataset len
          val entropyAv = AlgorithmUtils.calcEntropy(cnt, all)
          val infoAv = entropyAv * (all / dfCount)
          val splitInfoAv = AlgorithmUtils.entropyFormula(all / dfCount)

          (k, (infoAv, splitInfoAv)) // attribute, (info(v), splitInfo(v))
      }
      .reduceByKey((A0, A1) => (entropyGeneral - (A0._1 + A1._1) / (A0._2 + A1._2), 0.0))
      .map {
        case (k, (gainRatio, _)) =>
          (k, gainRatio)
      }
      .reduce((k1, k2) => if (k1._2 > k2._2) k1 else k2)._1 // Take the attribute with higher gain ratio

    println(attrTable
      .filter(_._1 == (aBest, 1.0))
      .collect().mkString("Array(", ", ", ")"))
  }
}