package Consolidator


import com.mongodb.DBObject
import com.mongodb.casbah.Imports.MongoClientURI
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.util.JSON
import const.Const
import Entry.DailyEntry.spark
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object ClientDetailConsolidator extends Consolidator {

  override  def consolidateRecord(ticker:String=null):DataFrame={
    val df:DataFrame=loadJSON(Const.client_detail)

    df.createOrReplaceTempView("client_detail")
    df.cache()

    //df.apply()
    df

  }




}
