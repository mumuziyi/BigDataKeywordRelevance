package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Using word preprocessor, remove all stop words and do stem in the content of the news article.
 */
public class NewsWordProcessor implements MapFunction<NewsArticle, NewsArticle> {

    private static final long serialVersionUID = -4631167868443368097L;
    @Override
    public NewsArticle call(NewsArticle value) throws Exception {
        List<ContentItem> original_contents = value.getContents();
        List<ContentItem> processed_contents = original_contents.stream().map(this::convert).collect(Collectors.toList());
        value.setContents(processed_contents);
        return value;
    }

    private ContentItem convert(ContentItem item){
        TextPreProcessor processor = new TextPreProcessor();
        List<String> processed_strings = processor.process(item.getContent());
        String content = processed_strings.toString();
        ContentItem return_value =  new ContentItem();
        return_value.setContent(content);
        return_value.setSubtype(item.getSubtype());
        return_value.setType(item.getType());
        return return_value;
    }
}