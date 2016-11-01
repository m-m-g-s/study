package mmgs.study.bigdata.spark.kwmatcher.storagehelper

import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick
import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.sql.SQLContext

object HBaseStorageHelper {

  def readTaggedClicks(sqlContext: SQLContext, filterDate: String) = {
    // TODO: Configuratiion is to be moved to global settings
    val configuration = new Configuration()
    val df = sqlContext.phoenixTableAsDataFrame(
      "LOG_TABLEF3"
      , Array("BID_ID", "TIMESTAMP_DATE", "TAGS_LIST", "LAT", "LON")
      // TODO: predicate is to be specified (by date, parametrized)
      //, predicate = Some("\"LAT\" = 40.6643")
      , predicate = Some("\"SUBSTR(TIMESTAMP_DATE, 0, 8)\" = '" + filterDate + "'")
      , conf = configuration
    )
    // debugging
    df.show()
    df.map(i => new TaggedClick(i(0).toString, i(1).toString, i(3).toString.toDouble, i(4).toString.toDouble, i(2).toString)).toJavaRDD()
  }
}
