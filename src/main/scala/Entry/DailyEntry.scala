package Entry



import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import Consolidator._
import _root_.Consolidator.DailyPriceConsolidator.loadCsv
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import const.Const
import org.apache.spark.sql.functions.lit



object DailyEntry extends App {

  //TODO ::: add log 4j  and a user log

  val spark = SparkSession.builder
    .master("local")
    .appName("stockopedia")
    .getOrCreate()


  val sc= spark.sparkContext



  DailyJob.initiateDailyIngestion(false)



}
