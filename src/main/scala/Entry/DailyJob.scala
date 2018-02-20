package Entry

//import Entry.DailyEntry.spark
import const.Const
import Consolidator._
object DailyJob {

  def initiateDailyIngestion(ticker:String=null,updateInventory:Boolean = false):Unit={

    if (ticker==null){
      val list=DailyEntry.spark.read.csv(Const.supportedCompany)

      list.rdd.collect.foreach(a=>{
        val ticker= a.toString().replaceAll("[","").replaceAll("]","")
        initiateConsolidators(ticker,updateInventory)
      })
    }
else
      initiateConsolidators(ticker,updateInventory)
  }


//TODO update this method when added new consolidators
  def initiateConsolidators(ticker:String=null,updateInventory: Boolean):Unit ={
    if (updateInventory)
    InventoryListConsolidator.consolidate(true,ticker)
    else
    InventoryListConsolidator.consolidate(false,ticker)
    DailyPriceConsolidator.consolidate(true,ticker)
    OptionsContractConsolidator.consolidate(true,ticker)
    ClientDetailConsolidator.consolidate(true,ticker)
    ClientTransactionConsolidator.consolidate(true,ticker)
  }


}
