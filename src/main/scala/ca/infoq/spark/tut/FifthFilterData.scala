package ca.infoq.spark.tut

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object FifthFilterData {
  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
 val automobiles =sqlContext.read.parquet("src\\main\\resources\\parquet\\autos.parquet").as[Auto]
  automobiles.filter(_.yearOfRegistration>2000).filter(_.price!= null).show()




  }
}
