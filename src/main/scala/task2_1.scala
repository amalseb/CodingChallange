import commons.{Common, Constants}
import org.apache.log4j.{Level, Logger}

object Task2_1 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = Common.getSparkSession(Constants.TASK2_1)

    val airbnbDf = Common.readParquetFile(spark, Constants.PART2_URL)

    // Print the first 5 in Dataframe to get an idea about the data.
    print(airbnbDf.show(5))
    spark.stop()
  }
}
