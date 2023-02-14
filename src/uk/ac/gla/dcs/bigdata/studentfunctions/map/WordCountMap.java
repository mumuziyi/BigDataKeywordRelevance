package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;
import org.terrier.matching.models.aftereffect.L;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.util.List;

public class WordCountMap implements MapFunction<NewsArticle, Tuple2<NewsArticle,Long>> {
    LongAccumulator fileAccount = new LongAccumulator();

    public WordCountMap(LongAccumulator longAccumulator){
        this.fileAccount = longAccumulator;
    }

    @Override
    public Tuple2<NewsArticle, Long> call(NewsArticle value) throws Exception {
        List<ContentItem> contentItems = value.getContents();
        long count = 0L;
        for (ContentItem contentItem: contentItems){
            if (contentItem.getContent().length() > 0){
//                System.out.println(contentItem.getContent());
                count += contentItem.getContent().split(" ").length;
                fileAccount.add(contentItem.getContent().split(" ").length);
            }
        }
        return new Tuple2<>(value,count);
    }
}
