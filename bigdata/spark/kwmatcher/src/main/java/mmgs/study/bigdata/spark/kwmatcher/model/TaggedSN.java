package mmgs.study.bigdata.spark.kwmatcher.model;

import java.io.Serializable;

/**
 *
 */
public class TaggedSN implements Serializable {
    private String id;
    private String day;
    private double longitude;
    private double latitude;
    private String tags;
    private String snKeywords;

    public TaggedSN(TaggedClick taggedClick, String snKeywords) {
        this.id = taggedClick.getId();
        this.day = taggedClick.getDay();
        this.longitude = taggedClick.getLongitude();
        this.latitude = taggedClick.getLatitude();
        this.tags = taggedClick.getTags();
        this.snKeywords = snKeywords;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getSnKeywords() {
        return snKeywords;
    }

    public void setSnKeywords(String snKeywords) {
        this.snKeywords = snKeywords;
    }
}
