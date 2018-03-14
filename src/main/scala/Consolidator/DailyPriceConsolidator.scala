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


object DailyPriceConsolidator extends Consolidator {


 override def consolidateRecord(ingest: Boolean, ticker: String): DataFrame ={

  val df_with_null= loadCsv(Const.workingDir+ticker+"/price.csv").withColumn("ticker",lit(ticker))

   println(ticker)

   if (df_with_null != null){
     val df=filterNull(df_with_null)

     val resultDf=  getThisYear(df).union(getPrevYear(df)).union(getThisMonth(df)).union(getPrevMonth(df)).union(getSnapShot(10,df,"tmp"))

     if (ingest){
       clearData("price",ticker)
       ingestDailyData(getThisYear(df),"price",ticker)
       ingestDailyData(getPrevYear(df),"price",ticker)
       ingestDailyData(getThisMonth(df),"price",ticker)
       ingestDailyData(getPrevMonth(df),"price",ticker)
       ingestDailyData(getSnapShot(10,df,"tmp"),"price",ticker)
     }
resultDf
   }
   else{
     null
   }
  }

  def clearData (collection:String,snapshot_type:String) :Unit ={
    val client=MongoClient(MongoClientURI(Const.mongoUrl))
    val mongoCollection= client(Const.database)(collection)
    mongoCollection.remove(MongoDBObject("ticker"->snapshot_type))
  }

 override def ingestDailyData(df:DataFrame,collection:String,snapshot_type:String): Unit = {
   val client=MongoClient(MongoClientURI(Const.mongoUrl))
   val mongoCollection= client(Const.database)(collection)
   df.toJSON.collect.foreach(a => {
     println(a.toString)
     mongoCollection.insert(JSON.parse(a.toString).asInstanceOf[DBObject])
   })
  }

  override def filterNull(df:DataFrame):DataFrame={
    val daily=Const.DaliyPrice
    df.select(checkForNull(col(daily.date.colName)).as(daily.date.colName),
      checkForNull(col(daily.open.colName)).as(daily.open.colName),
      checkForNull(col(daily.high.colName)).as(daily.high.colName),
      checkForNull(col(daily.low.colName)).as(daily.low.colName),
      checkForNull(col(daily.close.colName)).as(daily.close.colName),
      checkForNull(col(daily.volume.colName)).as(daily.volume.colName),
      col("ticker")
    )
  }



  final val getYearMonth=udf((date: String) => {
    val tmp:Array[String]=date.split("-")
    tmp(0)+tmp(1)
  })

  final val getYear=udf((date:String)=>{
    val tmp:Array[String]=date.split("-")
    tmp(0)
  })

  override val checkForNull: UserDefinedFunction = udf((col: String)=>{
    if (col==null)
      "0"
    else
      col
  })

  private def acquireMonth(df:DataFrame): String={
    val date=df.select(col(Const.DaliyPrice.date.colName)).first.toString.replace("[","").replace("]","").split("-")
    date(0)+date(1)
  }

  private def acquireYear(df:DataFrame):String={
    val date=df.select(col(Const.DaliyPrice.date.colName)).first.toString.replace("[","").replace("]","").split("-")
    date(0)
  }

  private val generateID= udf ((ticker:String, date:String,IDtype:String)=>{
    IDtype match {
      case "current_year" => ticker + date.replaceAll("-","") + "CY"
      case "prev_year" => ticker + date.replaceAll("-","") +  "PY"
      case "current_month" =>ticker + date.replaceAll("-","") + "CM"
      case "prev_month" =>ticker + date.replaceAll("-","") + "PM"
      case _ =>ticker + date.replaceAll("-","") + IDtype
    }
  })


  def getThisYear(df: DataFrame): DataFrame={
    val year=acquireYear(df)
    df.withColumn("year",getYear(col(Const.DaliyPrice.date.colName)))
        .withColumn("snapshot_type", lit("current_year"))
      .select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
        col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
        col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
        col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
        col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
        col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName),
        col("snapshot_type"),col("ticker"),
        generateID(col("ticker"),col(Const.DaliyPrice.date.colName),col("snapshot_type")).as("_id")
      ).where(col("year").equalTo(year))
  }

  def getPrevYear(df:DataFrame):DataFrame={
    df.show
    val year=acquireYear(df)
    val tmp=df.withColumn("year",getYear(col(Const.DaliyPrice.date.colName)))
      .select(col("year"),col(Const.DaliyPrice.date.colName),
        col(Const.DaliyPrice.open.colName),col(Const.DaliyPrice.high.colName),
        col(Const.DaliyPrice.low.colName),col(Const.DaliyPrice.close.colName),
        col(Const.DaliyPrice.volume.colName),col("ticker")
      ).where(col("year").notEqual(year))

    val prevYear=acquireYear(tmp)

    tmp.withColumn("snapshot_type", lit("prev_year"))
      .select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
      col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
      col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
      col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
      col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
      col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName),
      col("snapshot_type"),
        generateID(col("ticker"),col(Const.DaliyPrice.date.colName),col("snapshot_type")).as("_id")
        ,col("ticker"))
      .where(col("year").equalTo(prevYear))

  }


   def getThisMonth(df:DataFrame) :DataFrame={
    val date=this.acquireMonth(df)

    df.withColumn("monthYear",getYearMonth(col(Const.DaliyPrice.date.colName)) )
      .withColumn("snapshot_type", lit("current_month"))
      .select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
        col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
        col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
        col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
        col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
        col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName),
        col("snapshot_type"),col("ticker")
        ,generateID(col("ticker"),col(Const.DaliyPrice.date.colName),col("snapshot_type")).as("_id"))
      .where(col("monthYear").equalTo(date))
  }

   def getPrevMonth(df:DataFrame):DataFrame={
    val date=acquireMonth(df)
    val tmp= df.withColumn("monthYear",getYearMonth(col(Const.DaliyPrice.date.colName)) )
      .select(col("monthYear"),col(Const.DaliyPrice.date.colName),
        col(Const.DaliyPrice.open.colName),col(Const.DaliyPrice.high.colName),
        col(Const.DaliyPrice.low.colName),col(Const.DaliyPrice.close.colName),
        col(Const.DaliyPrice.volume.colName),
        col("ticker")
      ).where(col("monthYear").notEqual(date))
//tmp.show()
     val prevMonth=acquireMonth(tmp)
 //println(prevMonth)
     tmp.withColumn("snapshot_type",lit("prev_month"))
       .select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
       col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
       col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
       col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
       col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
       col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName),
         col("ticker"),col("snapshot_type"),
         generateID(col("ticker"),col(Const.DaliyPrice.date.colName).as("_id"),col("snapshot_type")).as("_id")
       ).where(col("monthYear").equalTo(prevMonth))
  }

   def getSnapShot(num:Int, df:DataFrame, snapshotType:String): DataFrame={
    df.withColumn("snapshot_type",lit(snapshotType))
      .select(col(Const.DaliyPrice.date.colName).as(Const.DaliyPrice.report.asOfDate.colName),
      col(Const.DaliyPrice.open.colName).as(Const.DaliyPrice.report.open.colName),
      col(Const.DaliyPrice.high.colName).as(Const.DaliyPrice.report.high.colName),
      col(Const.DaliyPrice.low.colName).as(Const.DaliyPrice.report.low.colName),
      col(Const.DaliyPrice.close.colName).as(Const.DaliyPrice.report.close.colName),
      col(Const.DaliyPrice.volume.colName).as(Const.DaliyPrice.report.volume.colName),
        generateID(col("ticker"),col(Const.DaliyPrice.date.colName),col("snapshot_type")).as("_id"),
        col("ticker"),col("snapshot_type")
       )
      .limit(num)
  }



}
