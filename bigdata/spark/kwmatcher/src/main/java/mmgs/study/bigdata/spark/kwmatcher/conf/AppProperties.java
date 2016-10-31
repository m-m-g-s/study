package mmgs.study.bigdata.spark.kwmatcher.conf;

/**
 * Created by Aliaksei_Neuski on 10/28/16.
 */
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;

@ConfigurationProperties
public class AppProperties implements Serializable {

    private SparkProp sparkProp = new SparkProp();
    private HiveProp hiveProp = new HiveProp();
    private MeetupProp meetupProp = new MeetupProp();

    public SparkProp getSparkProp() {
        return sparkProp;
    }

    public HiveProp getHiveProp() {
        return hiveProp;
    }

    public MeetupProp getMeetupProp() {
        return meetupProp;
    }

    public static class SparkProp implements Serializable {
        private String appName;
        private int duration;

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public int getDuration() {
            return duration;
        }

        public void setDuration(int duration) {
            this.duration = duration;
        }
    }

    public static class HiveProp implements Serializable {
        private String tableSavePath;

        public String getTableSavePath() {
            return tableSavePath;
        }

        public void setTableSavePath(String tableSavePath) {
            this.tableSavePath = tableSavePath;
        }
    }

    public static class MeetupProp implements Serializable {
        private String pathToKeys;

        public String getPathToKeys() {
            return pathToKeys;
        }

        public void setPathToKeys(String pathToKeys) {
            this.pathToKeys = pathToKeys;
        }

    }
}