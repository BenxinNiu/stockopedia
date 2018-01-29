package const

import com.typesafe.config.ConfigFactory

object Const {

   val config= ConfigFactory.load()

  val mongoUrl=config.getString("app.mongo.devUrl")
  val database=config.getString("app.mongo.database")

  val tickerList=config.getString("app.list.ticker")

   val prices=config.getString("app.location.prices")

   val inventory=config.getString("app.location.inventory")

  val options=config.getString("app.location.option")

   object DaliyPrice{
     val date=new CSVField("﻿\"DATE\"","as of date")
     val high=new CSVField("HIGH","highest price")
     val low=new CSVField("LOW","lowest price")
     val open=new CSVField("OPEN","open price")
     val close=new CSVField("CLOSE","closed price")
     val volume=new CSVField("VOLUME","trade volume")

     object report{
       val asOfDate= new CSVField("asOfDate","as of date")
       val high=new CSVField("high","highest price")
       val low=new CSVField("low","lowest price")
       val open=new CSVField("open","open price")
       val close=new CSVField("close","closed price")
       val volume=new CSVField("trade-volume","trade volume")
     }

   }


  object Options{
    val IDENTIFIER=new CSVField("﻿\"IDENTIFIER\"","Identifier")
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
       val TICKER=new CSVField("﻿\"TICKER\"","ticker (code)")
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
