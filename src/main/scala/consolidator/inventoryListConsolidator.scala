package consolidator

import const.Const
import main.scala.Entry.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf,col}



object inventoryListConsolidator extends Consolidator {

  def consolidateRecord(): DataFrame ={

   val df:DataFrame=spark.read.option("header", true).option("escape","\"").csv(Const.inventory)

    //df.show

   val inventoryDf= df.select(
      checkForNull(col(Const.Inventory.TICKER.colName)).as(Const.Inventory.Report.TICKER.colName),
      checkForNull(col(Const.Inventory.NAME.colName)).as(Const.Inventory.Report.NAME.colName),
      checkForNull(col(Const.Inventory.LEI.colName)).as(Const.Inventory.Report.LEI.colName),
      checkForNull(col(Const.Inventory.CIK.colName)).as(Const.Inventory.Report.CIK.colName),
      checkForNull(col(Const.Inventory.LATESTDATE.colName)).as(Const.Inventory.Report.LATESTDATE.colName)
    )
   inventoryDf.createOrReplaceTempView("inventory")
    inventoryDf.cache()
    inventoryDf
  }


}
