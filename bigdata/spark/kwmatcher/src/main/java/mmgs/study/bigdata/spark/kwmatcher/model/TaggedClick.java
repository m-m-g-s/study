package mmgs.study.bigdata.spark.kwmatcher.model;

import java.io.Serializable;

/**
 * Click tags source data
 */
public class TaggedClick implements Serializable {
    private String id;
    private String day;

    public TaggedClick() {
    }

    private double longitude;
    private double latitude;
    private String tags;

    public TaggedClick(String id, String day, double latitude, double longitude, String tags) {
        this.id = id;
        this.day = day;
        this.longitude = longitude;
        this.latitude = latitude;
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaggedClick that = (TaggedClick) o;

        if (Double.compare(that.longitude, longitude) != 0) return false;
        if (Double.compare(that.latitude, latitude) != 0) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (!day.equals(that.day)) return false;
        return tags.equals(that.tags);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id != null ? id.hashCode() : 0;
        result = 31 * result + day.hashCode();
        temp = Double.doubleToLongBits(longitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(latitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + tags.hashCode();
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
                '}';
    }

    public void clearId() {this.id = null;}

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
}