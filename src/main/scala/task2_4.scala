import commons.{common, constants}
import org.apache.spark.sql.functions.{max, min, sum}

object task2_4 {
  def main(args: Array[String]): Unit = {
    val spark = common.getSparkSession(constants.TASK2_3)

    val airbnbDf = common.readParquetFile(spark, constants.PART2_URL)

    val filteredDf = airbnbDf.filter(airbnbDf("price") === airbnbDf.agg(min("price")).first().getDouble(0) &&
      airbnbDf("review_scores_rating") === airbnbDf.agg(max("review_scores_rating")).first().getDouble(0))

    val op = filteredDf.select(sum(airbnbDf("beds")) + sum(("bedrooms")) * 2)
      .first()
      .getDouble(0)
      .toInt

    val out = new java.io.FileWriter(constants.TASK2_4_FILEPATH)
    out.write(op.toString)
    out.close()
  }

}
