package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ContentEssential;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsEssential;

import java.util.List;

public class NewsTransformation implements MapFunction<NewsArticle, NewsEssential>
{
    @Override
    public NewsEssential call(NewsArticle value) throws Exception
    {
        String id = value.getId();
        String title = value.getTitle();
        List<ContentItem> items = value.getContents();
        List<ContentEssential> contentEssentials = ContentEssential.convert(items);
        NewsEssential newsEssential1 = new NewsEssential(id, title, contentEssentials);
        return newsEssential1;
    }
}
