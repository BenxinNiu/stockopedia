
package main.scala



import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import consolidator._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import const.Const



object Entry extends App {

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


  //daliyPriceConsolidator.consolidateRecord("","")

//  spark.read.option("header", true).option("escape","\"").csv(Const.inventory).show

inventoryListConsolidator.consolidateRecord()
optionsContractConsolidator.consolidateRecord()

}