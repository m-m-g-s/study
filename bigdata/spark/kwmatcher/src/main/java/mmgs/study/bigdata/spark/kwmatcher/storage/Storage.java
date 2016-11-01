package mmgs.study.bigdata.spark.kwmatcher.storage;

import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

/**
 *
 */
public interface Storage {
    JavaRDD<TaggedClick> readTaggedClicks(SQLContext sqlContext, String filterDate);
}

