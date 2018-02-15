package Consolidator

import java.text.SimpleDateFormat
import java.util.Date

import Entry.DailyEntry.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.mongodb.casbah.{MongoClient,MongoCollection}
import com.mongodb.util.JSON
import com.mongodb.casbah.Imports._
import com.mongodb.DBObject

import const.Const

trait Consolidator {

  val sc = spark.sparkContext

  def consolidate(ticker:String=null): DataFrame = {
    consolidateRecord(ticker)
    null
  }

  def consolidateRecord(ticker:String=null):DataFrame={
 null
  }

//Test function
  def ingestDailyData(df:DataFrame,collection:String,snapshot_type:String): Unit = {
    val client=MongoClient(MongoClientURI(Const.mongoUrl))
    val mongoCollection= client(Const.database)(collection)
    mongoCollection.drop()
    //mongoCollection.remove(MongoDBObject(""->""))
      df.toJSON.collect.foreach(a => {
        mongoCollection.insert(JSON.parse(a.toString).asInstanceOf[DBObject])
      })

  }

  def loadCsv(path:String):DataFrame={
    spark.read.option("header", true).option("escape","\"").csv(path)
  }


  //d1 today's date
  def isAfterToday(d1:String,d2:String):Boolean={
    val dateFormat=new SimpleDateFormat("yyyy-MM-dd")
    if(dateFormat.parse(d1).compareTo(dateFormat.parse(d2))>0)
      true
    else
      false
  }

  final val checkForNull=udf((col: String)=>{
    if (col==null)
      "N/A"
    else
      col
  })

}

