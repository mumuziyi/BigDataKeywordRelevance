package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsEssential;

public class QueryProcessor implements MapFunction<Query, Query> {
    @Override
    public Query call(Query value) throws Exception {
        TextPreProcessor textPreProcessor = new TextPreProcessor();
        var original_query = value.getOriginalQuery();
        var processed_query = textPreProcessor.process(original_query).toString();
        return new Query(processed_query, value.getQueryTerms(), value.getQueryTermCounts());
    }
}
