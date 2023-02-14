package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.spark.sql.Dataset;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;

import java.io.Serializable;
import java.util.List;

public class NewsEssential implements Serializable {
    String id;
    String title;
    List<ContentEssential> contents;

    public NewsEssential(String id, String title, List<ContentEssential> contents) {
        this.id = id;
        this.title = title;
//        Convert input's List into a Dataset
        this.contents = contents;
    }
    public String getId() {
        return id;
    }
    public String getTitle() {
        return title;
    }
    public List<ContentEssential> getContents() {
        return contents;
    }
    }
