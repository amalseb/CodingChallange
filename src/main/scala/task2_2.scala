import commons.{Common, Constants}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, count, max, min}

object Task2_2 {

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = Common.getSparkSession(Constants.TASK2_2)

    val airbnbDf = Common.readParquetFile(spark, Constants.PART2_URL)

    // Computing the minimum price, maximum price, and total row count from this data set.
    val op = airbnbDf.agg(
      min(Constants.PRICE), max(Constants.PRICE), count("*")
    )

    // Converting the results to dataframe for the easiness of writing it to a file.
    val opDf = op
      .toDF(Constants.MIN_PRICE, Constants.MAX_PRICE, Constants.ROW_COUNT)

    // Writing output file under out/out_2_2.txt
    Common.writeAsTextFile(Constants.TASK2_2_FILEPATH, opDf)
    spark.stop()
  }
}
