import commons.{common, constants}
import org.apache.spark.sql.SparkSession

object task1_1{

  def main(args: Array[String]): Unit = {
    val spark = common.getSparkSession(constants.TASK1_1)

    val url = constants.PART1_URL

    val data = common.readCsvFromUrlAsList(spark: SparkSession, url:String)
    data.take(5).foreach(println)
  }
}
