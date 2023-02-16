package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class WordFrequency implements MapFunction<NewsArticle, Integer> {
    @Override
    public Integer call(NewsArticle newsArticle) throws Exception {
        return newsArticle.getContents().size();
    }
}
