package mmgs.study.bigdata.spark.kwmatcher.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Created by Aliaksei_Neuski on 10/28/16.
 */
@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties(AppProperties.class)
public class JavaConfig {

    @Bean
    public SparkConf sparkConf(AppProperties conf) {
        return new SparkConf().setAppName(conf.getSparkProp().getAppName());
    }

    @Bean
    public JavaStreamingContext streamingContext(SparkConf sparkConf, AppProperties conf) {
        int duration = conf.getSparkProp().getDuration();
        return new JavaStreamingContext(sparkConf, new Duration(duration));
    }
}