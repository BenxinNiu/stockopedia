package Entry



import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import Consolidator._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import const.Const


object DailyEntry extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("stockopedia")
    .getOrCreate()


  val sc= spark.sparkContext


  //  val conf: Configuration = sc.hadoopConfiguration
  //
  //  conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  //
  //  conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)


  //   val tickers:Array[String]=Const.tickerList.split(",")
  //
  //  tickers.foreach(a=>{
  //
  //  })

  //daliyPriceConsolidator.consolidate()
  inventoryListConsolidator.consolidate()
//optionsContractConsolidator.consolidate("AAPL")
}