package uk.ac.gla.dcs.bigdata.MyStructure;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class NewsDPHScore implements Serializable {
    private static final long serialVersionUID = -7802210882986773475L;

    NewsArticle newsArticle;

    Map<Query,Double> queryDoubleMap;

    public NewsDPHScore(NewsArticle newsArticle, Map<Query, Double> queryDoubleMap) {
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

    public Map<Query, Double> getQueryDoubleMap() {
        return queryDoubleMap;
    }

    public void setQueryDoubleMap(Map<Query, Double> queryDoubleMap) {
        this.queryDoubleMap = queryDoubleMap;
    }

    public NewsDPHScore() {
    }
}
