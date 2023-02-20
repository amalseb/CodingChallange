import commons.{Common, Constants}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, col}

object Task2_3 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = Common.getSparkSession(Constants.TASK2_3)

    val airbnbDf = Common.readParquetFile(spark, Constants.PART2_URL)

    // Calculating the average number of bathrooms and bedrooms across all the properties listed in this data set with a price of > 5000
    //and a review score being exactly equal to 10.
    // Here all review scores are taken into consideration. If either of the review is 10.0, then it is taken.
    val opDf = airbnbDf.select(Constants.BATHROOMS, Constants.BEDROOMS)
      .filter(airbnbDf(Constants.PRICE)> 5000 && airbnbDf(Constants.REVIEW_SCORE_ACCURACY) === 10.0
        || airbnbDf(Constants.REVIEW_SCORE_LOCATION) === 10.0 || airbnbDf(Constants.REVIEW_SCORE_CLEANLINESS) === 10.0 || airbnbDf(Constants.REVIEW_SCORE_CHECKIN) === 10.0
        || airbnbDf(Constants.REVIEW_SCORE_COMMUNICATION) === 10.0 || airbnbDf(Constants.REVIEW_SCORE_VALUE) === 10.0)
    .agg(
      avg(Constants.BATHROOMS).alias("avg_bathrooms"), avg(Constants.BEDROOMS).alias("avg_bedrooms"))

    // Writing the results into a CSV file out/out_2_3.txt
    Common.writeAsTextFile(Constants.TASK2_3_FILEPATH, opDf)
    spark.stop()
  }

}
