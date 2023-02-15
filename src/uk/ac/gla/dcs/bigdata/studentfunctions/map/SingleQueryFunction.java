package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class SingleQueryFunction implements MapFunction<Query, Query> {
    private static final long serialVersionUID = -4631167868443368097L;
    @Override
    public Query call(Query value) throws Exception {
        return value;
    }
}
