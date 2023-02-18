import commons.{common, constants}
import org.apache.spark.sql.functions.{col, count, max, min}

object task2_2 {

  def main(args: Array[String]): Unit = {
    val spark = common.getSparkSession(constants.TASK2_2)

    val airbnbDf = common.readParquetFile(spark, constants.PART2_URL)

    val op = airbnbDf.agg(
      min("price"), max("price"), count("*")
    )

    val opDf = op
      .toDF("min_price", "max_price", "row_count")

    common.writeAsTextFile(constants.TASK2_2_FILEPATH, opDf)

  }
}
