package uk.ac.gla.dcs.bigdata.newstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;
import java.util.Map;

/**
 * A data structure that holds the count of each query's term in article and the total length of the article.
 */
public class NewsDPHScore implements Serializable {
    private static final long serialVersionUID = -7802210882986773475L;

    NewsArticle newsArticle;

    Map<String ,Double> queryDoubleMap;

    /**
     * @param newsArticle The {@link NewsArticle} object it contains.
     * @param queryDoubleMap a map keeps the count of each query's term in the news article. i.e. the number of times each query term appears in the article.
     */
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
