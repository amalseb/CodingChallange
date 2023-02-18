package commons
import org.apache.spark.rdd.RDD

import scala.io.Source
import org.apache.spark.sql.{SparkSession}

object common {

  def getSparkSession(appName: String): SparkSession={
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local") // Set the master URL to "local" for testing purposes
      .getOrCreate()
    spark
  }

  def readCsvFromUrlAsList(spark: SparkSession, url:String): RDD[String]={
    val dataLines = Source.fromURL(url).getLines().toList
    val dataRDD = spark.sparkContext.parallelize(dataLines)
    dataRDD
  }

  def writeAsTextFile(filepath: String, data: RDD[String]) = {
    data.saveAsTextFile(filepath)
  }

}
