package mmgs.study.bigdata.spark.kwmatcher.storage;

import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick;
import mmgs.study.bigdata.spark.kwmatcher.storagehelper.HBaseStorageHelper;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SQLContext;


/**
 *
 */
public class HBaseStorage implements Storage {
    @Override
    public JavaRDD<TaggedClick> readTaggedClicks(SQLContext sqlContext, String filterDate) {
        return HBaseStorageHelper.readTaggedClicks(sqlContext, filterDate);
    }
}
