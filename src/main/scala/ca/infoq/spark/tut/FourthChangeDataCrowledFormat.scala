package ca.infoq.spark.tut

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try


object FourthChangeDataCrowledFormat {
  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
 val automobiles =sqlContext.read.parquet("src\\main\\resources\\parquet\\autos.parquet").as[Auto]
    automobiles.filter(_.dateCrawled != null).map(a=>a.copy(dateCrawled=toSimpleDate(a.dateCrawled))).show()

  }
  def toSimpleDate(dateString: String): String = {
    val parser = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    Try { LocalDateTime.parse(dateString, parser) }.toOption .map(_.format(formatter)).get }
}
