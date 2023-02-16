package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.SimplifiedNewsArticle;

import java.util.List;

public class ProcessNewsArticle implements MapFunction<NewsArticle, SimplifiedNewsArticle> {
    @Override
    public SimplifiedNewsArticle call(NewsArticle value) throws Exception {
        TextPreProcessor processor = new TextPreProcessor();


        List<ContentItem> contentItems = value.getContents();
        for (ContentItem contentItem : contentItems) {
            // process content
            List<String> temp = processor.process(contentItem.getContent());
            StrBuilder contentSB = new StrBuilder();
            for (String str : temp) {
                contentSB.append(str + " ");
            }
            contentItem.setContent(contentSB.toString());
        }


        String original = value.getTitle();
        List<String> title = processor.process(value.getTitle());
        StrBuilder titleSB = new StrBuilder();
        for (String str : title) {
            titleSB.append(str + " ");
        }
        if (value.getTitle() == null) {
            value.setContents(null);
            return new SimplifiedNewsArticle(value.getId(), value.getContents(), original, titleSB.toString());
        }
        return new SimplifiedNewsArticle(value.getId(), value.getContents(), original, titleSB.toString());

//        return value;
    }
}
