package consolidator

import const.Const
import main.scala.Entry.spark
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col,udf}


object daliyPriceConsolidator extends Consolidator {


  final val getYearMonth=udf((date: String) => {
    val tmp:Array[String]=date.split("-")
    tmp(0)+tmp(1)
  })

  final val getYear=udf((date:String)=>{
    val tmp:Array[String]=date.split("-")
    tmp(0)
  })

 def consolidateRecord(ticker:String, fullName:String): DataFrame ={

  val df: DataFrame = spark.read.option("header", true).option("escape","\"").csv(Const.prices)
//df.columns.foreach(println)
   getPrevYear(df).collect().foreach(println)
 df
  }

  private def acquireMonth(df:DataFrame): String={
    val date=df.select(col(Const.DaliyPrice.date.colName)).first.toString.replace("[","").replace("]","").split("-")
    date(0)+date(1)
  }

  private def acquireYear(df:DataFrame):String={
    val date=df.select(col(Const.DaliyPrice.date.colName)).first.toString.replace("[","").replace("]","").split("-")
    date(0)
  }


  def getThisYear(df: DataFrame): Dataset[String]={
    val year=acquireYear(df)
    df.withColumn("year",getYear(col(Const.DaliyPrice.date.colName)))
      .select(col(Const.DaliyPrice.date.colName).as("asOfDate"),
        col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.open.colName),col("HIGH").as("high"),
        col("LOW").as("low"),col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.close.colName),col("VOLUME").as("trade-volume")
      ).where(col("year").equalTo(year)).toJSON
  }

  def getPrevYear(df:DataFrame):Dataset[String]={
    val year=acquireYear(df)
    val tmp=df.withColumn("year",getYear(col(Const.DaliyPrice.date.colName)))
      .select(col("year"),col(Const.DaliyPrice.date.colName),
        col(Const.DaliyPrice.open.colName),col("HIGH"),
        col("LOW"),col(Const.DaliyPrice.close.colName),col("VOLUME")
      ).where(col("year").notEqual(year))

    val prevYear=acquireYear(tmp)

    tmp.select(col(Const.DaliyPrice.date.colName).as("asOfDate"),
      col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.open.colName),col("HIGH").as("high"),
      col("LOW").as("low"),col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.close.colName),col("VOLUME").as("trade-volume"))
      .where(col("year").equalTo(prevYear)).toJSON

  }


   def getThisMonth(df:DataFrame) :Dataset[String]={
    val date=this.acquireMonth(df)

    df.withColumn("monthYear",getYearMonth(col(Const.DaliyPrice.date.colName)) )
      .select(col(Const.DaliyPrice.date.colName).as("asOfDate"),
        col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.open.colName),col("HIGH").as("high"),
        col("LOW").as("low"),col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.close.colName),col("VOLUME").as("trade-volume")
      ).where(col("monthYear").equalTo(date)).toJSON
  }

   def getPrevMonth(df:DataFrame):Dataset[String]={
    val date=this.acquireMonth(df)
     println(date)
    val tmp= df.withColumn("monthYear",getYearMonth(col(Const.DaliyPrice.date.colName)) )
      .select(col("monthYear"),col(Const.DaliyPrice.date.colName),
        col(Const.DaliyPrice.open.colName),col("HIGH"),
        col("LOW"),col(Const.DaliyPrice.close.colName),col("VOLUME")
      ).where(col("monthYear").notEqual(date))
//tmp.show()
     val prevMonth=this.acquireMonth(tmp)
 //println(prevMonth)
     tmp.select(col(Const.DaliyPrice.date.colName).as("asOfDate"),
       col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.open.colName),col("HIGH").as("high"),
         col("LOW").as("low"),col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.close.colName),col("VOLUME").as("trade-volume")
       ).where(col("monthYear").equalTo(prevMonth)).toJSON
  }

   def getSnapShot(num:Int, df:DataFrame): Dataset[String]={
    df.select(col(Const.DaliyPrice.date.colName).as("asOfDate"),
      col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.open.colName),col("HIGH").as("high"),
      col("LOW").as("low"),col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.close.colName),col("VOLUME").as("trade-volume")).limit(num).toJSON
  }



}
