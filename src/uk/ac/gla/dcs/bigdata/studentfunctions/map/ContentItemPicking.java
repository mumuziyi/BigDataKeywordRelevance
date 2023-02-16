package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.util.List;
import java.util.stream.Collectors;

// Only keep contentItem that has sub-type "paragraph", and only keep the top five contentItems
public class ContentItemPicking implements MapFunction<NewsArticle, NewsArticle> {

        private static final long serialVersionUID = -4631167868443368097L;
        @Override
        public NewsArticle call(NewsArticle value) throws Exception {
            List<ContentItem> contents = value.getContents();
//            Filter out the contentItems that has sub-type "paragraph", considering the sub-type could be null
            List<ContentItem> new_contents = contents.stream().filter(item -> item.getSubtype() != null && item.getSubtype().equals("paragraph")).collect(Collectors.toList());
//            Only keep the top five contentItems
            if (new_contents.size() > 5) {
                new_contents = new_contents.subList(0, 5);
            }
            value.setContents(new_contents);
            return value;
        }
}
