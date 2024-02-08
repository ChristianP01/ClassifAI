import data.CleanedDataFrame
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    // Start Spark session
    val spark = SparkSession
      .builder
      .appName("ClassifAI")
      .master("local[1]")
      .getOrCreate()

    val filePath = "./src/main/assets/Context.csv"
    var df = spark.read.option("header", value = true).csv(filePath)

    // Dataframe creation
    val preprocessor = new CleanedDataFrame(spark, df)

    // Pre processing
    df = preprocessor.getDataFrame

    df.show()
    println(df.first())
  }
}