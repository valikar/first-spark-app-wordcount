package ca.infoq.spark.tut

import com.google.gson.Gson
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class newTab(dataCrawled:String,name:String,carInfo:String)
case class kjson(price:Integer,vehicleType:String,gearbox:String)
object EightNewTable {
  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
 val automobiles =sqlContext.read.parquet("src\\main\\resources\\parquet\\autos.parquet").as[Auto]


   val firstSubTable =automobiles.filter(_.dateCrawled!= null).filter(_.name!= null).filter(_.price!= null).filter(_.price>100).filter(_.vehicleType!= null)
   .filter(_.gearbox!= null).map(a=>newTab(a.dateCrawled,a.name,toStro(kjson(a.price-100,a.vehicleType,a.gearbox)))).as[newTab]
val finals = automobiles.filter(_.price!= null).filter(_.price>=2100).join(firstSubTable,automobiles.col("name")===firstSubTable.col("name") ).show()


  }
  def toStro(inp:kjson): String = {
    val json=new Gson()
    return json.toJson(inp)

  }
}
