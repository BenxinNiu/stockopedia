package consolidator

import main.scala.Entry.spark
import org.apache.spark.sql.DataFrame

trait Consolidator {

  val sc= spark.sparkContext

  def consolidate(): DataFrame ={


    null
  }


  def ingest():Unit={


  }



}
