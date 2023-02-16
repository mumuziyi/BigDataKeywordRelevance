package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.util.List;

public class ProcessNewsArticle implements MapFunction<NewsArticle,NewsArticle> {
    @Override
    public NewsArticle call(NewsArticle value) throws Exception {
        TextPreProcessor processor = new TextPreProcessor();

        if (value.getTitle() == null){
            value.setContents(null);
            return value;
        }

        List<ContentItem> contentItems = value.getContents();
        for (ContentItem contentItem : contentItems){
            // process content
            List<String> temp = processor.process(contentItem.getContent());
            StrBuilder contentSB = new StrBuilder();
            for (String str : temp){
                contentSB.append(str + " ");
            }
            contentItem.setContent(contentSB.toString());
        }


        return value;
    }
}
