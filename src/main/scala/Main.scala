import data.CleanedDataFrame
import org.apache.spark.sql.DataFrame
object Main {
  def main(args: Array[String]): Unit = {
    val df = new CleanedDataFrame()

    df.removeTopicErrors().removePunctuations().tokenize().removeStopWords()
  }
}