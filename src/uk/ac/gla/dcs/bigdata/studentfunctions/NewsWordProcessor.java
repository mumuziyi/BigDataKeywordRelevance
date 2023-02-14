package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.ContentEssential;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsEssential;

import java.util.List;
import java.util.stream.Collectors;

public class NewsWordProcessor implements MapFunction<NewsEssential, NewsEssential> {

    private static final long serialVersionUID = -4631167868443368097L;
    @Override
    public NewsEssential call(NewsEssential value) throws Exception {
        List<ContentEssential> original_contents = value.getContents();
        List<ContentEssential> processed_contents = original_contents.stream().map(ContentEssential::convert).collect(Collectors.toList());
        return new NewsEssential(value.getId(), value.getTitle(), processed_contents);
    }
}