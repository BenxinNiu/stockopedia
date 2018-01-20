package consolidator

import java.text.SimpleDateFormat
import java.util.Date

import main.scala.Entry.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import com.mongodb.DBObject

trait Consolidator {

  val sc = spark.sparkContext

  def consolidate(): DataFrame = {


    null
  }


  def ingest(): Unit = {


  }

  //d1 today's date
  def isAfterToday(d1:String,d2:String):Boolean={
    val dateFormat=new SimpleDateFormat("yyyy-MM-dd")
    if(dateFormat.parse(d1).compareTo(dateFormat.parse(d2))>0)
      true
    else
      false
  }

  final val checkForNull=udf((col: String)=>{
    if (col==null)
      "N/A"
    else
      col
  })

}
//
//def acquireDate()={
//  new SimpleDateFormat("yyyy-MM").format(new Date())
//}
