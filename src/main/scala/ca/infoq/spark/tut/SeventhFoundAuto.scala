package ca.infoq.spark.tut

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SeventhFoundAuto {
  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
 val automobiles =sqlContext.read.parquet("src\\main\\resources\\parquet\\autos.parquet").as[Auto]
        val cnt=automobiles.filter(_.price!= null).filter(_.dateCrawled!= null).filter(_.dateCreated!= null).filter(_.gearbox!= null).filter(_.gearbox.startsWith("automatik"))
      .map(a=>a.copy(nrOfPictures=toData(a.dateCrawled).getSecond -(a.dateCreated.toLocalDateTime.getSecond)))
         .sort("nrOfPictures").reduce((a,b)=>(b))
    println(cnt)
    //automobiles.filter((automobiles("price")>50000)).show() //as dataframe






  }
  def toData(dateString: String): LocalDateTime = {
    val parser = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
     return LocalDateTime.parse(dateString, parser)}
}
