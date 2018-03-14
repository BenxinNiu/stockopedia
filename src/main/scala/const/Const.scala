package const

import com.typesafe.config.ConfigFactory

object Const {

   val config= ConfigFactory.load()

  val mongoUrl=config.getString("app.mongo.devUrl")

  val database=config.getString("app.mongo.database")

  val workingDir =config.getString("app.location.home")

  val tickerList=config.getString("app.list.ticker")

   val prices=config.getString("app.location.prices")

   val inventory=config.getString("app.location.inventory")

   val options=config.getString("app.location.option")

   val supportedCompany=config.getString("app.location.supportedCompany")

   val client_detail=config.getString("app.location.client_detail")

   val client_trans=config.getString("app.location.client_trans")

  object clients_acc{
    val date=new CSVField("asofdate","as of date")
    val ticker=new CSVField("ticker","ticker")
    val companyName=new CSVField("company_name","full name of company")
    val volume=new CSVField("volume","share volume")
    val ID=new CSVField("client","client id num")
    val value=new CSVField("value_usd","total value in usd")
    val account=new CSVField("acc_type","usd")
  }

  object trans{
    val transType= new CSVField("trans_type","")
    val book_value= new CSVField("book_value","")
    val ask_price= new CSVField("ask_price","")
    val fullfilled= new CSVField("fullfilled","")
    val ticker= new CSVField("ticker","")
    val trans_volume= new CSVField("trans_volume","")
    val client_id= new CSVField("client_id","")
    val trans_date= new CSVField("trans_date","")
    val fullfilled_date= new CSVField("fullfilled_date","")
    val ID=new CSVField("_id","trans id num")
  }


   object DaliyPrice{
     val date=new CSVField("DATE","as of date")
     val high=new CSVField("HIGH","highest price")
     val low=new CSVField("LOW","lowest price")
     val open=new CSVField("OPEN","open price")
     val close=new CSVField("CLOSE","closed price")
     val volume=new CSVField("VOLUME","trade volume")

     object report{
       val asOfDate= new CSVField("asofDate","as of date")
       val high=new CSVField("high","highest price")
       val low=new CSVField("low","lowest price")
       val open=new CSVField("open","open price")
       val close=new CSVField("close","closed price")
       val volume=new CSVField("trade_volume","trade volume")
     }

   }


  object Options{
    val IDENTIFIER=new CSVField("IDENTIFIER","Identifier")
    val TICKER=new CSVField("TICKER","ticker (code)")
    val EXPIR=new CSVField("EXPIRATION","contract expiration date")
    val STRIKE=new CSVField("STRIKE","strike price")
    val TYPE=new CSVField("TYPE","contract type")

    object report{
      val IDENTIFIER=new CSVField("identifier","Identifier")
      val TICKER=new CSVField("ticker","ticker (code)")
      val EXPIR=new CSVField("expiration","contract expiration date")
      val STRIKE=new CSVField("strike","strike price")
      val TYPE=new CSVField("type","contract type")
    }

  }

  object Inventory{
       val TICKER=new CSVField("TICKER","ticker (code)")
       val NAME=new CSVField("NAME","company full name")
       val LEI=new CSVField("LEI","Legal Entity Identifier")
       val CIK=new CSVField("CIK","Central Index Key")
       val LATESTDATE=new CSVField("LATEST_FILING_DATE","latest filing date")

    object Report{
      val TICKER=new CSVField("ticker","ticker (code)")
      val NAME=new CSVField("company_name","company full name")
      val LEI=new CSVField("legal_entity_identifier","Legal Entity Identifier")
      val CIK=new CSVField("central_index_key","Central Index Key")
      val LATESTDATE=new CSVField("latest_filing_date","latest filing date")

    }
  }

}
