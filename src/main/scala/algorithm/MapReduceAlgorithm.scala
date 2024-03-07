package algorithm

import org.apache.spark.sql.{DataFrame, Row}

class MapReduceAlgorithm {
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
    val attrTable = df.rdd.flatMap { row =>
      row
        .toSeq
        .zipWithIndex
        .slice(2, row.toSeq.length)
        .filter(_._1 == "1")
        .map {
          case (_, idx) =>
          (row.schema.fieldNames(idx), (row(0).toString.toInt, row(1).toString))
        }
    }

    println(attrTable.collect().mkString("Array(", ", ", ")"))
  }
}
