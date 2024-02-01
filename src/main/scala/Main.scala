import data.preprocessing
import org.apache.spark.sql.DataFrame
object Main {
  def main(args: Array[String]): Unit = {
    val preproc = new preprocessing()
    // val ds: DataFrame = preproc.cleanDataset()
    preproc.cleanDataset()

  }
}