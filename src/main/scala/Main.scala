import data.DataframeCleaner
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Start Spark session
    val spark = SparkSession
      .builder
      .appName("ClassifAI")
      .master("local[*]")
      .getOrCreate()

    val filePath = "./src/main/assets/Context.csv"
    var df = spark.read.option("header", value = true).csv(filePath)

    // Dataframe creation
    val preprocessor = new DataframeCleaner(spark, df)

    // Pre processing
    df = preprocessor.getPivotedDataFrame

    preprocessor.getOccurrenceMap
  }
}