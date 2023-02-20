import commons.{Common, Constants}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Task1_2 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = Common.getSparkSession(Constants.TASK1_2)

    val url = Constants.PART1_URL

    val data = Common.readCsvFromUrlAsList(spark: SparkSession, url: String)


    /*
    Subsection A
     */

    //list of all (unique) products present in the transactions.
    val opDataA = data.map(_.split(",")).flatMap(row => row)
      .distinct()

    //Write out this list to a text file: out/out_1_2a.txt
    Common.writeAsTextFile(Constants.TASK1_2A_FILEPATH, opDataA)

    /*
    Subsection B
     */

    //total count of products
    val opDataB = spark.sparkContext.parallelize(Seq(s"Count: ${opDataA.count()}"))

    //Write out this list to a text file: out/out_1_2b.txt
    Common.writeAsTextFile(Constants.TASK1_2B_FILEPATH, opDataB)
    spark.stop()
  }

}
