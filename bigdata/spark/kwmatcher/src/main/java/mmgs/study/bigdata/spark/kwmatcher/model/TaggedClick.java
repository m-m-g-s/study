package mmgs.study.bigdata.spark.kwmatcher.model;

import java.io.Serializable;

/**
 * Click tags source data
 */
public class TaggedClick implements Serializable {
    private String id;
    private String day;
    private double longitude;
    private double latitude;
    private String tags;
    private long impression;

    public TaggedClick() {
    }

    public TaggedClick(String id, String day, double latitude, double longitude, String tags, long impression) {
        this.id = id;
        this.day = day;
        this.longitude = longitude;
        this.latitude = latitude;
        this.tags = tags;
        this.impression = impression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaggedClick that = (TaggedClick) o;

        return Double.compare(that.longitude, longitude) == 0
                && Double.compare(that.latitude, latitude) == 0
                && impression == that.impression
                && id.equals(that.id)
                && day.equals(that.day)
                && tags.equals(that.tags);

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
        result = 31 * result + (int) (impression ^ (impression >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "TaggedClick{" +
                "id='" + id + '\'' +
                ", day='" + day + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", tags='" + tags + '\'' +
                ", impression='" + impression + '\'' +
                '}';
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

    public long getImpression() {
        return this.impression;
    }

    public void setImpression(Long impression) {
        this.impression = impression;
    }
}