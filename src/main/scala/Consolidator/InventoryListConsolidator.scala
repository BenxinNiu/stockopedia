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

   val df=loadCsv(Const.inventory)
    //df.show

   if (df!=null){
     val supportedCompany = loadCsv(Const.supportedCompany)

     val inventoryDf= df.withColumn("_id",monotonically_increasing_id()).
       select(
         col("_id"),
         checkForNull(col(Const.Inventory.TICKER.colName)).as(Const.Inventory.Report.TICKER.colName),
         checkForNull(col(Const.Inventory.NAME.colName)).as(Const.Inventory.Report.NAME.colName),
         checkForNull(col(Const.Inventory.LEI.colName)).as(Const.Inventory.Report.LEI.colName),
         checkForNull(col(Const.Inventory.CIK.colName)).as(Const.Inventory.Report.CIK.colName),
         checkForNull(col(Const.Inventory.LATESTDATE.colName)).as(Const.Inventory.Report.LATESTDATE.colName)
       )

     val ingest_df = inventoryDf.join(supportedCompany,col(Const.Inventory.TICKER.colName)===col("name"),"left").select(
       checkForNull(col(Const.Inventory.Report.TICKER.colName)).as(Const.Inventory.Report.TICKER.colName),
       checkForNull(col(Const.Inventory.Report.NAME.colName)).as(Const.Inventory.Report.NAME.colName),
       checkForNull(col(Const.Inventory.Report.LEI.colName)).as(Const.Inventory.Report.LEI.colName),
       checkForNull(col(Const.Inventory.Report.CIK.colName)).as(Const.Inventory.Report.CIK.colName),
       checkForNull(col(Const.Inventory.Report.LATESTDATE.colName)).as(Const.Inventory.Report.LATESTDATE.colName),
       col("supported")
     )

     inventoryDf.createOrReplaceTempView("inventory")
     inventoryDf.cache()

     if(ingest)
       ingestDailyData(ingest_df, "inventory", null)

     inventoryDf
   }
   else {
     null
   }
  }


  override def ingestDailyData(df:DataFrame,collection:String,snapshot_type:String) :Unit ={
    val client=MongoClient(MongoClientURI(Const.mongoUrl))
    val mongoCollection= client(Const.database)(collection)
   mongoCollection.drop()
    df.toJSON.collect.foreach(a => {
      println(a.toString)
      mongoCollection.insert(JSON.parse(a.toString).asInstanceOf[DBObject])
    })
  }

}
