package Consolidator

import Consolidator.DailyPriceConsolidator.loadCsv
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

object ClientTransactionConsolidator extends Consolidator {

  override def consolidateRecord(ingest: Boolean, ticker: String = null):DataFrame={

    var client_detail:DataFrame=null
    if(spark.catalog.isCached("client_detail")) {
      client_detail = spark.sql("SELECT * FROM client_detail")
      spark.catalog.clearCache()
    }
    else
      client_detail = ClientDetailConsolidator.consolidateRecord(false, ticker)

    val client_trans:DataFrame= loadJSON(Const.client_trans)

    val priceDf:DataFrame= loadCsv(Const.prices).withColumn("ticker",lit(ticker))

    //val snapshotPriceDf:DataFrame= DailyPriceConsolidator.getSnapShot(30,priceDf,"analye").select(Const.DaliyPrice.close.colName)

   val jointDf= transJoinClients(client_trans,client_detail,ticker)

    val df = analyzeBuy(priceDf,jointDf)
    df.show

    null
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

  def analyzeSell(priceDf:DataFrame,jointDf:DataFrame):DataFrame={
    null  //TODO :: think of this logic
  }



  def analyzeBuy(priceDf:DataFrame,jointDf:DataFrame):DataFrame={

    val df=jointDf.select("*").where(col(Const.trans.transType.colName)==="buy"&&
                                   col(Const.trans.fullfilled.colName)==="true")
    val processedDf=jointDf.join(priceDf,priceDf(Const.DaliyPrice.date.colName)===jointDf(Const.trans.trans_date.colName),"left")
        .withColumn("analysis",getDifference(jointDf(Const.trans.fullfilled.colName),
                                             jointDf(Const.trans.book_value.colName),
                                             jointDf(Const.trans.ask_price.colName),
                                             priceDf(Const.DaliyPrice.close.colName),
                                             priceDf(Const.DaliyPrice.open.colName)))
                      .select(col("analysis"),col(Const.DaliyPrice.close.colName),col(Const.DaliyPrice.open.colName),
                        jointDf(Const.trans.ID.colName), jointDf(Const.trans.ask_price.colName),
                        jointDf(Const.trans.book_value.colName), jointDf(Const.trans.client_id.colName))
    processedDf
  }


  def transJoinClients(trans:DataFrame,clients:DataFrame,ticker:String):DataFrame={
    trans.join(clients,trans(Const.trans.client_id.colName)===clients(Const.clients_acc.ID.colName),"left")
      .select("*").where(trans("ticker")===ticker)
  }

  val getDifference=udf((fullfilled:String,bookValue:String,askPrice:String,close:String, open:String)=>{
    fullfilled match {
      case "true"=> {val percentage= bookValue.toDouble / close.toDouble
                      if (percentage <1 && percentage > 0.9 )
                       "good deal 10 percent "
                      else if(percentage <1 && percentage > 0.7 )
                        "a bit risky decision (short sell \\?) (almost 30 percent drop)"
                      else if (percentage >1)
                         "bad decision!!"
                      else "N/A"
      }
      case "false"=> { val percentage= askPrice.toDouble / close.toDouble
                       if (percentage <1 && percentage > 0.9 )
                           "expect to fall 10 percent "
                       else if(percentage <1 && percentage > 0.7 )
                            "planning to short it (almost 30 percent drop)"
                       else if (percentage >1)
                             "what r u doing"
                       else "N/A"}
    }
  })


}
