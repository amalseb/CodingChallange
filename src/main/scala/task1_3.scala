import commons.{common, constants}
import org.apache.spark.sql.SparkSession

object task1_3 {
  def main(args: Array[String]): Unit = {
    val spark = common.getSparkSession(constants.TASK1_3)

    val url = constants.PART1_URL

    val data = common.readCsvFromUrlAsList(spark: SparkSession, url: String)

    val top5Data = data
      .flatMap(line => line.split(","))
      .map(product => (product, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)

    val opData = spark.sparkContext.parallelize((top5Data)
      .map { case (product, count) => s"$product:$count" })
    common.writeAsTextFile(constants.TASK1_3_FILEPATH, opData)
  }
}
