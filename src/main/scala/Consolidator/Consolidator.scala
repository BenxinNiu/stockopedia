package Consolidator

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import Entry.DailyEntry.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.mongodb.casbah.{MongoClient, MongoCollection}
import com.mongodb.util.JSON
import com.mongodb.casbah.Imports._
import com.mongodb.DBObject
import const.Const

trait Consolidator {

  val sc = spark.sparkContext

  def consolidate(ingest: Boolean,ticker:String=null): DataFrame = {
    consolidateRecord(ingest, ticker)

  }

  def consolidateRecord(ingest: Boolean, ticker: String = null):DataFrame={
 null
  }

//Test function
  def ingestDailyData(df:DataFrame,collection:String,snapshot_type:String): Unit = {
    val client=MongoClient(MongoClientURI(Const.mongoUrl))
    val mongoCollection= client(Const.database)(collection)
    //mongoCollection.remove(MongoDBObject(""->""))
      df.toJSON.collect.foreach(a => {
        mongoCollection.insert(JSON.parse(a.toString).asInstanceOf[DBObject])
      })

  }

  def loadCsv(path:String):DataFrame={

    val file= new File(path.substring(7))

    if(file.length==0 || !file.exists()) {
      null
    }
    else {
      val df = spark.read.option("header", true).option("escape", "\"").csv(path)
      df
    }
  }

  def loadJSON(path:String):DataFrame={
    spark.read.json(path)
  }

  //d1 today's date
  def isAfterToday(d1:String,d2:String):Boolean={
    val dateFormat=new SimpleDateFormat("yyyy-MM-dd")
    if(dateFormat.parse(d1).compareTo(dateFormat.parse(d2))>0)
      true
    else
      false
  }

  def filterNull(df:DataFrame):DataFrame={
    null
  }


   val checkForNull=udf((col: String)=>{
    if (col==null)
      "N/A"
    else
      col
  })

  def downloadFile(ticker:String,file:String):Unit= {
    val dest= "/home/benxin/projects/dailyDownload/raw/"
    val url="https://0f731094cd8305b1859398def068acba:d4589289cabd7f987f85861a3f55c787@api.intrinio.com/"
    try {
      val src = scala.io.Source.fromURL(url+file+".csv?identifier=" + ticker)
      val out = new java.io.FileWriter(dest+ticker+ "/" +file+".csv")
      out.write(src.mkString)
      out.close
      println("download")
    } catch {
      case e: java.io.IOException => "error occured"
    }
  }


}

