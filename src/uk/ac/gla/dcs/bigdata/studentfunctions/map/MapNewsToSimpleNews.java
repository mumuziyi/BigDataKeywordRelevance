package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.SimplifiedNewsArticle;

import java.util.List;

public class MapNewsToSimpleNews implements MapFunction<NewsArticle, SimplifiedNewsArticle> {
    @Override
    public SimplifiedNewsArticle call(NewsArticle value) throws Exception {

        String original = value.getTitle();
        TextPreProcessor processor = new TextPreProcessor();
        // process title
        List<String> title = processor.process(value.getTitle());
        StrBuilder titleSB = new StrBuilder();
        for (String str : title){
            titleSB.append(str + " ");
        }


        return new SimplifiedNewsArticle(value.getId(),value.getContents(),original,titleSB.toString());
    }
}
