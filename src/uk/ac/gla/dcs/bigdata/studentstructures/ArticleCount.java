package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class ArticleCount {
    public NewsArticle Article;
    public int ArticleLength;

    public short TermCount;

    public ArticleCount(NewsArticle article, int articleLength, short termCount) {
        Article = article;
        ArticleLength = articleLength;
        TermCount = termCount;
    }
}
