package consolidator

import const.Const
import main.scala.Entry.spark
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col,udf}


object daliyPriceConsolidator extends Consolidator {


 def consolidateRecord(ticker:String, fullName:String): DataFrame ={

  val df: DataFrame = spark.read.option("header", true).option("escape","\"").csv(Const.prices)
getThisYear(df).show
 df
  }

  final val getYearMonth=udf((date: String) => {
    val tmp:Array[String]=date.split("-")
    tmp(0)+tmp(1)
  })

  final val getYear=udf((date:String)=>{
    val tmp:Array[String]=date.split("-")
    tmp(0)
  })

  private def acquireMonth(df:DataFrame): String={
    val date=df.select(col(Const.DaliyPrice.date.colName)).first.toString.replace("[","").replace("]","").split("-")
    date(0)+date(1)
  }

  private def acquireYear(df:DataFrame):String={
    val date=df.select(col(Const.DaliyPrice.date.colName)).first.toString.replace("[","").replace("]","").split("-")
    date(0)
  }


  def getThisYear(df: DataFrame): DataFrame={
    val year=acquireYear(df)
    df.withColumn("year",getYear(col(Const.DaliyPrice.date.colName)))
      .select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
        col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
        col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
        col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
        col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
        col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName)
      ).where(col("year").equalTo(year))
  }

  def getPrevYear(df:DataFrame):DataFrame={
    val year=acquireYear(df)
    val tmp=df.withColumn("year",getYear(col(Const.DaliyPrice.date.colName)))
      .select(col("year"),col(Const.DaliyPrice.date.colName),
        col(Const.DaliyPrice.open.colName),col(Const.DaliyPrice.high.colName),
        col(Const.DaliyPrice.low.colName),col(Const.DaliyPrice.close.colName),
        col(Const.DaliyPrice.volume.colName)
      ).where(col("year").notEqual(year))

    val prevYear=acquireYear(tmp)

    tmp.select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
      col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
      col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
      col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
      col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
      col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName))
      .where(col("year").equalTo(prevYear))

  }


   def getThisMonth(df:DataFrame) :DataFrame={
    val date=this.acquireMonth(df)

    df.withColumn("monthYear",getYearMonth(col(Const.DaliyPrice.date.colName)) )
      .select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
        col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
        col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
        col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
        col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
        col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName))
      .where(col("monthYear").equalTo(date))
  }

   def getPrevMonth(df:DataFrame):DataFrame={
    val date=this.acquireMonth(df)
     println(date)
    val tmp= df.withColumn("monthYear",getYearMonth(col(Const.DaliyPrice.date.colName)) )
      .select(col("monthYear"),col(Const.DaliyPrice.date.colName),
        col(Const.DaliyPrice.open.colName),col(Const.DaliyPrice.high.colName),
        col(Const.DaliyPrice.low.colName),col(Const.DaliyPrice.close.colName),
        col(Const.DaliyPrice.volume.colName)
      ).where(col("monthYear").notEqual(date))
//tmp.show()
     val prevMonth=this.acquireMonth(tmp)
 //println(prevMonth)
     tmp.select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
       col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
       col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
       col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
       col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
       col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName)
       ).where(col("monthYear").equalTo(prevMonth))
  }

   def getSnapShot(num:Int, df:DataFrame): DataFrame={
    df.select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
      col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
      col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
      col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
      col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
      col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName))
      .limit(num)
  }



}
