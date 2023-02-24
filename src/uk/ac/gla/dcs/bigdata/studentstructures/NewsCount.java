package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;
import java.util.Map;

/**
 * A data structure that holds the count of each query's term in article and the total length of the article.
 * Each article will have a NewsCount object.
 * Suppose there are 5000 articles in the original data file, there'll be 5000 NewsCount objects exists during processing.
 */
public class NewsCount implements Serializable {

    private static final long serialVersionUID = 6467111084687728905L;

    NewsArticle newsArticle;

    Map<String, Integer> termCountMap;

    int totalLength;

    /**
     * @param newsArticle  The {@link NewsArticle} object it contains.
     * @param termCountMap a map keeps the count of each query's term in the news article. i.e. the number of times each query term appears in the article.
     * @param totalLength  The total length of the news article. i.e. the number of terms in the article.
     */
    public NewsCount(NewsArticle newsArticle, Map<String, Integer> termCountMap, int totalLength) {
        super();
        this.newsArticle = newsArticle;
        this.termCountMap = termCountMap;
        this.totalLength = totalLength;
    }

    public NewsCount() {
    }

    public int getTotalLength() {
        return totalLength;
    }

    public void setTotalLength(int totalLength) {
        this.totalLength = totalLength;
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
