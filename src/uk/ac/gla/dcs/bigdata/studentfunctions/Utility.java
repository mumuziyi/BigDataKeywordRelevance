package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.sql.Dataset;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class Utility {
//    Count the total words in the article
    public static int countArticleLength(NewsArticle newsArticle){
        int length = 0;
        for (ContentItem content : newsArticle.getContents()) {
            length += content.getContent().split(" ").length;
        }
        return length;
    }

    public static int termInDatasetFrequency(String term, Dataset<NewsArticle> news){
        int count = 0;
        for (NewsArticle newsArticle : news.collectAsList()) {
            for (ContentItem content : newsArticle.getContents()) {
                if (content.getContent().contains(term)) {
                    count++;
                    break;
                }
            }
        }
        return count;
    }

}
