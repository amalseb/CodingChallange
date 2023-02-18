import commons.{common, constants}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object task1_2 {
  def main(args: Array[String]): Unit = {
    val spark = common.getSparkSession(constants.TASK1_2)

    val url = constants.PART1_URL

    val data = common.readCsvFromUrlAsList(spark: SparkSession, url: String)


    //    Subsection A
    val opDataA = data.map(_.split(",")).flatMap(row => row)
      .distinct()
    common.writeAsTextFile(constants.TASK1_2A_FILEPATH, opDataA)

    //    Subsection B
    val opDataB = spark.sparkContext.parallelize(Seq(s"Count: ${opDataA.count()}"))
    common.writeAsTextFile(constants.TASK1_2B_FILEPATH, opDataB)

  }

}
