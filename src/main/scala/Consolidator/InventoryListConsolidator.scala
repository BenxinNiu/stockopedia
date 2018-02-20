package Consolidator

import const.Const
import Entry.DailyEntry.spark
import com.mongodb.DBObject
import com.mongodb.casbah.Imports.MongoClientURI
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.util.JSON
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._//{col, udf}



object InventoryListConsolidator extends Consolidator {

 override def consolidateRecord(ingest: Boolean, ticker: String = null): DataFrame ={

   val df:DataFrame=spark.read.option("header", true).option("escape","\"").csv(Const.inventory)

    //df.show

   val inventoryDf= df.withColumn("_id",monotonically_increasing_id()).
     select(
       col("_id"),
      checkForNull(col(Const.Inventory.TICKER.colName)).as(Const.Inventory.Report.TICKER.colName),
      checkForNull(col(Const.Inventory.NAME.colName)).as(Const.Inventory.Report.NAME.colName),
      checkForNull(col(Const.Inventory.LEI.colName)).as(Const.Inventory.Report.LEI.colName),
      checkForNull(col(Const.Inventory.CIK.colName)).as(Const.Inventory.Report.CIK.colName),
      checkForNull(col(Const.Inventory.LATESTDATE.colName)).as(Const.Inventory.Report.LATESTDATE.colName)
    )
   inventoryDf.createOrReplaceTempView("inventory")
    inventoryDf.cache()

   if(ingest)
     ingestDailyData(inventoryDf, "inventory", null)

       inventoryDf
  }


  override def ingestDailyData(df:DataFrame,collection:String,snapshot_type:String) :Unit ={
    val client=MongoClient(MongoClientURI(Const.mongoUrl))
    val mongoCollection= client(Const.database)(collection)
   mongoCollection.drop()
    df.toJSON.collect.foreach(a => {
      println(a)
      mongoCollection.insert(JSON.parse(a.toString).asInstanceOf[DBObject])
    })
  }

}
