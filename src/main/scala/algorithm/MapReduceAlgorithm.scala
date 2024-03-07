package algorithm

import org.apache.spark.sql.{DataFrame, Row}

class MapReduceAlgorithm {
  /** a, row_id, c */
  private var attrTable: Seq[(String, Int, String)] = Seq.empty
  /** a, c, cnt */
  val countTable: Map[String, (String, Int)] = Map.empty
  /** node_id (hash), row_id */
  val hashTable: Seq[(String, Int)] = Seq.empty

  /**
   * Algorithm 2: Data preparation (executed one time only)
   * The first thing we need is to convert the traditional relational table based data
   * into above three data structure for further  MapReduce processing.
   */
  def dataPreparation(df: DataFrame): Unit = {
    /** Populate attrTable */
    attrTable = df.collect().flatMap { row => {
      row
        .toSeq
        .zipWithIndex
        .slice(2, row.toSeq.length)
        .filter(_._1 == "1")
        .map { elem =>
          (df.columns(elem._2), row(0).toString.toInt, row(1).toString)
        }
      }
    }
  }
}
