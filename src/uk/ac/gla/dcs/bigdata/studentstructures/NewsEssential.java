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

    public short getQueryFrequency(String query) {
        short frequency = 0;
        for (ContentEssential content : contents) {
//            If the content contains the query string, increase the frequency by 1
            if (content.getContent().contains(query)) {
                frequency++;
            }
        }
        return frequency;
    }

    public int getLength(){
        int length = 0;
        for (ContentEssential content : contents) {
            length += content.getContent().split(" ").length;
        }
        return length;
    }
}
