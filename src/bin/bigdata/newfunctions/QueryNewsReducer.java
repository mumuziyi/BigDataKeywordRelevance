package bin.bigdata.newfunctions;

import org.apache.spark.api.java.function.ReduceFunction;
import bin.bigdata.providedstructures.RankedResult;
import bin.bigdata.newstructures.QueryNewsListStructure;

import java.util.*;

/**
 * This class is used to reduce QueryNewsListStructure according to the original query.
 * connect tow List if they have same original query
 */

public class QueryNewsReducer implements ReduceFunction<QueryNewsListStructure> {
    private static final long serialVersionUID = -4158919171891652902L;

    @Override
    public QueryNewsListStructure call(QueryNewsListStructure v1, QueryNewsListStructure v2) throws Exception {
        Map<String, List<RankedResult>> queryListMap1 = v1.getQueryListMap();
        Map<String, List<RankedResult>> queryListMap2 = v2.getQueryListMap();

        Map<String, List<RankedResult>> newMap = new HashMap<>();

        Set<String> queries = queryListMap1.keySet();

        for (String query : queries) {

            List<RankedResult> join = new ArrayList<>();

//            Consider the case that both element are not null
            if (queryListMap1.get(query) != null && queryListMap2.get(query) != null) {
                join.addAll(queryListMap1.get(query));
                join.addAll(queryListMap2.get(query));
            } else if (queryListMap1.get(query) != null) { // The second is null and the first is not null
                join.addAll(queryListMap1.get(query));
            } else if (queryListMap2.get(query) != null) { // The first is null and the second is not null
                join.addAll(queryListMap2.get(query));
            }

            newMap.put(query, join);
        }
        return new QueryNewsListStructure(newMap);
    }
}
