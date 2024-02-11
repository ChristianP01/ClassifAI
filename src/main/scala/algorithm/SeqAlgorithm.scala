package algorithm
import model.AlgorithmUtils
import org.apache.spark.sql.DataFrame

class SeqAlgorithm(df: DataFrame) extends AlgorithmUtils {

  var classes: List[String] = List.empty

//  df.select("Context/Topic").distinct().foreach( row =>
//    classes += row.get(0).toString
//  )

//  val (totalEntropy, classMap) = calcEntropy(df, classes)


}
