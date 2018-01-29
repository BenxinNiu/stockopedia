package consolidator

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import const.Const
import main.scala.Entry.spark
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}


object optionsContractConsolidator extends Consolidator {


  def consolidateRecord(): DataFrame ={

    var inventoryDf:DataFrame=null
    if(spark.catalog.isCached("inventory")) {
      inventoryDf = spark.sql("SELECT * FROM inventory")
     spark.catalog.clearCache()
    }
    else
      inventoryDf=inventoryListConsolidator.consolidateRecord()

   val optionContractDf=getOptionDataFrame()

   val jointDf:DataFrame= optionJoinInventory(optionContractDf,inventoryDf)

  val filteredDf:DataFrame=filterExpiredContract(jointDf,"effective")

    filteredDf
  }


  val trim=udf((date:String)=>{
    date.toString
  })

  val compareDates = udf((expiDate:String)=>{
     val today:String=new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val dateFormat=new SimpleDateFormat("yyyy-MM-dd")
    if(isAfterToday(today,expiDate.toString))
      expiDate
    else
      "effective"
  })

  def filterExpiredContract(jointDf:DataFrame,condition:String):DataFrame={


   val finalDf= jointDf.withColumn("status", compareDates(col(Const.Options.report.EXPIR.colName)))
      .select(
        col("status").as("isExpired"),
        col(Const.Options.report.IDENTIFIER.colName),
        col(Const.Inventory.Report.NAME.colName),
        col(Const.Options.report.TICKER.colName),
        col(Const.Options.report.EXPIR.colName),
        col(Const.Options.report.STRIKE.colName),
        col(Const.Options.report.TYPE.colName),
        col(Const.Inventory.Report.LEI.colName),
        col(Const.Inventory.Report.CIK.colName)
      ).where(col("isExpired").notEqual(col(Const.Options.report.EXPIR.colName)))
    finalDf
  }

  def getCallTypeNonExpired(jointDf:DataFrame):DataFrame={
    jointDf.select("*").where(col(Const.Options.report.TYPE.colName).equalTo("call"))

  }

  def getPutTypeNonExpired(jointDf:DataFrame):DataFrame={
    jointDf.select("*").where(col(Const.Options.report.TYPE.colName).equalTo("put"))
  }

  def getOptionDataFrame():DataFrame={

    val df=spark.read.option("header", true).option("escape","\"").csv(Const.options)
    df.select(
      col(Const.Options.IDENTIFIER.colName).as(Const.Options.report.IDENTIFIER.colName),
      col(Const.Options.TICKER.colName).as(Const.Options.report.TICKER.colName),
      col(Const.Options.EXPIR.colName).as(Const.Options.report.EXPIR.colName),
      col(Const.Options.STRIKE.colName).as(Const.Options.report.STRIKE.colName),
      col(Const.Options.TYPE.colName).as(Const.Options.report.TYPE.colName)
    )
  }

  def optionJoinInventory(optionDF:DataFrame,inventory:DataFrame):DataFrame={

    optionDF.createOrReplaceTempView("option")
    inventory.createOrReplaceTempView("inventory")
     val sql= new StringBuilder()
       .append("SELECT ")
       .append(Const.Options.report.IDENTIFIER.colName +",")
       .append(Const.Inventory.Report.NAME.colName + ",")
       .append("option."+Const.Options.report.TICKER.colName + ",")
       .append(Const.Options.report.EXPIR.colName + ",")
       .append(Const.Options.report.STRIKE.colName + ",")
       .append(Const.Options.report.TYPE.colName + ",")
       .append(Const.Inventory.Report.LEI.colName + ",")
       .append(Const.Inventory.Report.CIK.colName )
       .append(" FROM option LEFT JOIN inventory ")
       .append("ON option."+Const.Options.report.TICKER.colName)
       .append(" = inventory."+Const.Inventory.Report.TICKER.colName)
     //  .append(" ;")
    spark.sql(sql.toString())
  }


}
