package Entry

//import Entry.DailyEntry.spark
import const.Const
import Consolidator._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
object DailyJob {

  def initiateDailyIngestion(updateInventory:Boolean,ticker:String=null):Unit={

    if (ticker==null){
      val list=DailyPriceConsolidator.loadCsv(Const.supportedCompany).select(col("name"))

      if (updateInventory)
        InventoryListConsolidator.consolidate(true,ticker)


//      val new_list= ArrayBuffer[String]
//
//      list.rdd.foreach(a=>{
//        val ticker=a.toString.replaceAll("\\[","").replaceAll("\\]","")
//      })
//

      list.rdd.collect.foreach(a=>{
        val ticker= a.toString().replaceAll("\\[","").replaceAll("\\]","")
        initiateConsolidators(ticker)
      })
    }
else
      initiateConsolidators(ticker)
  }


//TODO update this method when added new consolidators
  def initiateConsolidators(ticker:String):Unit ={
   // InventoryListConsolidator.consolidate(false,ticker)
    DailyPriceConsolidator.consolidate(true,ticker)
   // OptionsContractConsolidator.consolidate(true,ticker)
   // ClientDetailConsolidator.consolidate(true,ticker)
   // ClientTransactionConsolidator.consolidate(true,ticker)
  }


}
