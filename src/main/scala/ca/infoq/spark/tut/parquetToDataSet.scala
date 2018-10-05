package ca.infoq.spark.tut

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object parquetToDataSet {
  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    val autos = sqlContext.read.parquet("src\\main\\resources\\parquet\\autos.parquet")
//    val count = sqlContext.read.parquet("src\\main\\resources\\parquet\\count.parquet")
//    autos.show()
//    count.show()
    import sqlContext.implicits._
 val automobiles =sqlContext.read.parquet("src\\main\\resources\\parquet\\autos.parquet").as[Auto]
  //  automobiles.show()
//val counts =sqlContext.read.parquet("src\\main\\resources\\parquet\\count.parquet").as[Count]
//    counts.show()
//automobiles.map(a =>a.copy(name=a.name.replace('_',' '))).show()
//automobiles.filter(_.yearOfRegistration>2000).filter(_.price>=0).show()

//    val parser =DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//    val formatter =DateTimeFormatter.ofPattern("yyyy/MM/dd")
def toSimpleDate(dateString: String): String = {
  val parser = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
  Try { LocalDateTime.parse(dateString, parser) }.toOption .map(_.format(formatter)).get }

    automobiles.filter(_.dateCrawled != null).map(a=>a.copy(dateCrawled=toSimpleDate(a.dateCrawled))).show()

//  val x =automobiles.filter(_.price!= null).filter(_.brand.startsWith("ford")).filter(_.seller.startsWith("privat")).count()
//
//    println(x)

//    automobiles.filter(_.price!= null).filter(_.gearbox!= null).filter(_.gearbox.startsWith("automatik")).map(a=>a.copy(nrOfPictures=a.dateCrawled.toLocalDateTime.compareTo(a.dateCreated.toLocalDateTime)))
//      .sort("nrOfPictures").show()


  }
}
