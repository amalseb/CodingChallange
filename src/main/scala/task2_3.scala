import commons.{common, constants}
import org.apache.spark.sql.functions.{avg, col}

object task2_3 {
  def main(args: Array[String]): Unit = {
    val spark = common.getSparkSession(constants.TASK2_3)

    val airbnbDf = common.readParquetFile(spark, constants.PART2_URL)

    val opDf = airbnbDf.select("bathrooms", "bedrooms")
      .filter(airbnbDf("price")> 5000 && airbnbDf("review_scores_accuracy") === 10.0 || airbnbDf("review_scores_accuracy") === 10.0
        || airbnbDf("review_scores_location") === 10.0 || airbnbDf("review_scores_cleanliness") === 10.0 || airbnbDf("review_scores_checkin") === 10.0
        || airbnbDf("review_scores_communication") === 10.0 || airbnbDf("review_scores_value") === 10.0)
    .agg(
      avg("bathrooms").alias("avg_bathrooms"), avg("bedrooms").alias("avg_bedrooms"))

    common.writeAsTextFile(constants.TASK2_3_FILEPATH, opDf)
  }

}
