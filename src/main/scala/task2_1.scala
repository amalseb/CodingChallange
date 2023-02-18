import commons.{common, constants}

object task2_1 {
  def main(args: Array[String]): Unit = {
    val spark = common.getSparkSession(constants.TASK2_1)

    val airbnbDf = common.readParquetFile(spark, constants.PART2_URL)
    print(airbnbDf.show(5))
  }
}
