package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.util.List;

/**
 * This Map Function takes in a raw query and use the TextPreProcessor to process the query.
 * Producing a new query with stop words removed and stemmed.
 */
public class ProcessQuery implements MapFunction<Query, Query> {
    @Override
    public Query call(Query value) throws Exception {
        TextPreProcessor processor = new TextPreProcessor();
        List<String> ProcessedList = processor.process(value.getOriginalQuery());
        StrBuilder sb = new StrBuilder();
        for (String str : ProcessedList) {
            sb.append(str + " ");
        }
        value.setOriginalQuery(sb.toString());
        return value;
    }
}
