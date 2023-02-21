package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;
import java.util.Map;

public class NewsDPHScore implements Serializable {
    private static final long serialVersionUID = -7802210882986773475L;

    NewsArticle newsArticle;

    Map<String ,Double> queryDoubleMap;

    public NewsDPHScore(NewsArticle newsArticle, Map<String, Double> queryDoubleMap) {
        super();
        this.newsArticle = newsArticle;
        this.queryDoubleMap = queryDoubleMap;
    }

    public NewsArticle getNewsArticle() {
        return newsArticle;
    }

    public void setNewsArticle(NewsArticle newsArticle) {
        this.newsArticle = newsArticle;
    }

    public Map<String, Double> getQueryDoubleMap() {
        return queryDoubleMap;
    }

    public void setQueryDoubleMap(Map<String, Double> queryDoubleMap) {
        this.queryDoubleMap = queryDoubleMap;
    }

    public NewsDPHScore() {
    }
}
