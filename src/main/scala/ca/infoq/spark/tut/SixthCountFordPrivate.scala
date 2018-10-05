package ca.infoq.spark.tut

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SixthCountFordPrivate {
  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
 val automobiles =sqlContext.read.parquet("src\\main\\resources\\parquet\\autos.parquet").as[Auto]
      val countFordPrivate =automobiles.filter(_.price!= null).filter(_.brand.startsWith("ford")).filter(_.seller.startsWith("privat")).count()
  println(countFordPrivate)




  }
}
