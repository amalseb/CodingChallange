package commons
import org.apache.spark.rdd.RDD

import scala.io.Source
import org.apache.spark.sql.{DataFrame, SparkSession}

object Common {
/*
Function for building spark session.
 */
  def getSparkSession(appName: String): SparkSession={
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local") // Set the master URL to "local" for testing purposes in local environment.
      .getOrCreate()
    spark
  }
/*
Read CSV from URL as list and returns RDD[String].
 */
  def readCsvFromUrlAsList(spark: SparkSession, url:String): RDD[String]={
    val dataLines = Source.fromURL(url).getLines().toList
    val dataRDD = spark.sparkContext.parallelize(dataLines)
    dataRDD
  }

  /*
  Write RDD[String] to text file in specified path.
   */
  def writeAsTextFile(filepath: String, data: RDD[String]) = {
    data.saveAsTextFile(filepath)
  }

  /*
  Read parquet file.
   */
  def readParquetFile(spark:SparkSession, path:String) = {
    spark.read.parquet(path)
  }

  /*
  Write dataframe to to text file.
   */
  def writeAsTextFile(filepath: String, data: DataFrame)={
    data.write.mode("overwrite").option("header", "true").csv(filepath)
  }

}
