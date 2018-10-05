package ca.infoq.spark.tut

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SecondSaveDatasetAsParket {
  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
 val automobiles =sqlContext.read.parquet("src\\main\\resources\\parquet\\autos.parquet").as[Auto]
  automobiles.show()
val counts =sqlContext.read.parquet("src\\main\\resources\\parquet\\count.parquet").as[Count]
   counts.show()



  }
}
