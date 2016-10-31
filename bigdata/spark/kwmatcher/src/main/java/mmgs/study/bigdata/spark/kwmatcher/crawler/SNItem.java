package mmgs.study.bigdata.spark.kwmatcher.crawler;

import java.io.Serializable;

/**
 * POJO for Social Network event
 */
public class SNItem implements Serializable {
    private static final String ID = "id";
    private static final String DESCRIPTION = "description";

    private String id;
    private String description;

    public SNItem() {
    }

    public SNItem(String id, String description) {
        this.id = id;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "SNItem{" +
                "id='" + id + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}