import commons.{Common, Constants}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Task1_3 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = Common.getSparkSession(Constants.TASK1_3)

    val url = Constants.PART1_URL

    val data = Common.readCsvFromUrlAsList(spark: SparkSession, url: String)

    // Taking top 5 Data from RDD
    val top5Data = data
      .flatMap(line => line.split(","))
      .map(product => (product, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)

    val opData = spark.sparkContext.parallelize((top5Data)
      .map { case (product, count) => s"$product:$count" })

    //the results out in descending order of frequency into a file out/out_1_3.txt
    Common.writeAsTextFile(Constants.TASK1_3_FILEPATH, opData)
    spark.stop()
  }
}
