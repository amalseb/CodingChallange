import commons.{Common, Constants}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Task1_1{

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = Common.getSparkSession(Constants.TASK1_1)

    val url = Constants.PART1_URL

    val data = Common.readCsvFromUrlAsList(spark: SparkSession, url:String)

    // Print first 5 elements from RDD
    data.take(5).foreach(println)

    spark.stop()
  }
}
