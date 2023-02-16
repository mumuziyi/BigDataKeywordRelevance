package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.commons.lang.text.StrBuilder;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.io.Serializable;
import java.util.List;

public class SimplifiedNewsArticle implements Serializable {
    private String id;
    private List<ContentItem> contents;
    private String originalTitle;
    private String filteredTitle;
    public SimplifiedNewsArticle(){};

    public SimplifiedNewsArticle(String id, List<ContentItem> contents, String originalTitle, String filteredTitle) {
        this.id = id;
        this.contents = contents;
        this.originalTitle = originalTitle;
        this.filteredTitle = filteredTitle;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<ContentItem> getContents() {
        return contents;
    }

    public void setContents(List<ContentItem> contents) {
        this.contents = contents;
    }

    public String getOriginalTitle() {
        return originalTitle;
    }

    public void setOriginalTitle(String originalTitle) {
        this.originalTitle = originalTitle;
    }

    public String getFilteredTitle() {
        return filteredTitle;
    }

    public void setFilteredTitle(String filteredTitle) {
        this.filteredTitle = filteredTitle;
    }
}
