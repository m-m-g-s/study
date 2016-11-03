package mmgs.study.bigdata.spark.kwmatcher.storagehelper

import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick
import org.apache.phoenix.spark._
import org.apache.spark.sql.SQLContext

object HBaseStorageHelper {

  def readTaggedClicks(sqlContext: SQLContext, zookeeperQuorum: String, filterDate: String) = {
    val df = sqlContext.phoenixTableAsDataFrame(
      "LOG_TABLEF3"
      , Array("USER_TAGS", "TIMESTAMP_DATE", "TAGS_LIST", "LAT", "LON")
      , predicate = Some("\"TIMESTAMP_DATE\" like '" + filterDate + "%'")
      , zkUrl = Some(zookeeperQuorum)
    )
    // debugging
    df.show()
    df.map(i => new TaggedClick(i(0).toString, i(1).toString, i(3).toString.toDouble, i(4).toString.toDouble, i(2).toString, 1)).toJavaRDD()
  }
}
