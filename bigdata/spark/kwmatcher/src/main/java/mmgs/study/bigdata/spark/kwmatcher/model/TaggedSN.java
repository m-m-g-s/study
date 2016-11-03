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
    private long impression;

    public TaggedSN(TaggedClick taggedClick, String snKeywords) {
        this.id = taggedClick.getId();
        this.day = taggedClick.getDay();
        this.longitude = taggedClick.getLongitude();
        this.latitude = taggedClick.getLatitude();
        this.tags = taggedClick.getTags();
        this.snKeywords = snKeywords;
        this.impression = taggedClick.getImpression();
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

    public long getImpression() {
        return impression;
    }

    public void setImpression(long impression) {
        this.impression = impression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaggedSN taggedSN = (TaggedSN) o;

        return Double.compare(taggedSN.longitude, longitude) == 0
                && Double.compare(taggedSN.latitude, latitude) == 0
                && id.equals(taggedSN.id)
                && day.equals(taggedSN.day)
                && tags.equals(taggedSN.tags)
                && snKeywords.equals(taggedSN.snKeywords);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id.hashCode();
        result = 31 * result + day.hashCode();
        temp = Double.doubleToLongBits(longitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(latitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + tags.hashCode();
        result = 31 * result + snKeywords.hashCode();
        return result;
    }
}
