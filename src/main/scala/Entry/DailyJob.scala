package Entry

//import Entry.DailyEntry.spark
import const.Const
import Consolidator._
object DailyJob {

  def initiateDailyIngestion(ticker:String=null,updateInventory:Boolean = false):Unit={
    if (updateInventory)
      InventoryListConsolidator.consolidate()

    if (ticker==null){
      val list=DailyEntry.spark.read.csv(Const.supportedCompany)

      list.rdd.collect.foreach(a=>{
        val ticker= a.toString().replaceAll("[","").replaceAll("]","")
        initiateConsolidators(ticker)
      })
    }
else
      initiateConsolidators(ticker)
  }

//TODO update this method when added new consolidators
  def initiateConsolidators(ticker:String=null):Unit ={
    DailyPriceConsolidator.consolidate(ticker)
    OptionsContractConsolidator.consolidate(ticker)
    ClientDetailConsolidator.consolidate(ticker)
    ClientTransactionConsolidator.consolidate(ticker)
  }


}
