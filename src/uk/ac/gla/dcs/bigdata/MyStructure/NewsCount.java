package uk.ac.gla.dcs.bigdata.MyStructure;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewsCount implements Serializable {

    private static final long serialVersionUID = 6467111084687728905L;

    NewsArticle newsArticle;

    Map<String,Integer> termCountMap;

    int totalLength;

    public NewsCount(NewsArticle newsArticle, Map<String,Integer> termCountMap, int totalLength){
        super();
        this.newsArticle = newsArticle;
        this.termCountMap = termCountMap;
        this.totalLength = totalLength;
    }

    public int getTotalLength() {
        return totalLength;
    }

    public void setTotalLength(int totalLength) {
        this.totalLength = totalLength;
    }

    public NewsCount() {
    }

    public NewsArticle getNewsArticle() {
        return newsArticle;
    }

    public void setNewsArticle(NewsArticle newsArticle) {
        this.newsArticle = newsArticle;
    }

    public Map<String, Integer> getTermCountMap() {
        return termCountMap;
    }

    public void setTermCountMap(Map<String, Integer> termCountMap) {
        this.termCountMap = termCountMap;
    }
}
