import commons.{Common, Constants}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{max, min, sum}

object Task2_4 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = Common.getSparkSession(Constants.TASK2_4)

    val airbnbDf = Common.readParquetFile(spark, Constants.PART2_URL)

    // Finding the property with the lowest price and highest rating.
    val filteredDf = airbnbDf.filter(airbnbDf(Constants.PRICE) === airbnbDf.agg(min(Constants.PRICE)).first().getDouble(0) &&
      airbnbDf(Constants.REVIEW_SCORE_RATING) === airbnbDf.agg(max(Constants.REVIEW_SCORE_RATING)).first().getDouble(0))

    // Takes guess that each room can take two people
    val op = filteredDf.select(sum(airbnbDf(Constants.BEDS)) + sum((Constants.BEDROOMS)) * 2)
      .first()
      .getDouble(0)
      .toInt

    // Writing the output to the file.
    val out = new java.io.FileWriter(Constants.TASK2_4_FILEPATH)
    out.write(op.toString)
    out.close()
    spark.stop()
  }

}
