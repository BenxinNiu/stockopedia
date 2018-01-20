package const

import com.typesafe.config.ConfigFactory

object Const {

   val config= ConfigFactory.load()

   val prices=config.getString("app.location.prices")

   object DaliyPrice{
     val date=new CSVField("﻿\"DATE\"","as of date")
     val high=new CSVField("HIGH","highest price")
     val low=new CSVField("LOW","lowest price")
     val open=new CSVField("OPEN","open price")
     val close=new CSVField("CLOSE","closed price")
     val volume=new CSVField("VOLUME","trade volume")
   }

}
