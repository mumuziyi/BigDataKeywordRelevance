package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;

import java.io.Serializable;
import java.util.List;

public class NewsEssential implements Serializable {
    String id;
    String title;
    List<ContentItem> contents;

    public NewsEssential(String id, String title, List<ContentItem> contents) {
        this.id = id;
        this.title = title;
        this.contents = contents;
    }
    public String getId() {
        return id;
    }
    public String getTitle() {
        return title;
    }
    public List<ContentItem> getContents() {
        return contents;
    }
    }
