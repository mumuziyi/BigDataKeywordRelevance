package uk.ac.gla.dcs.bigdata.MyMaps;

import org.apache.spark.api.java.function.ReduceFunction;
import uk.ac.gla.dcs.bigdata.MyStructure.QueryNewsListStructure;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

import java.util.*;

public class QueryNewsReduce implements ReduceFunction<QueryNewsListStructure> {
    private static final long serialVersionUID = -4158919171891652902L;

    @Override
    public QueryNewsListStructure call(QueryNewsListStructure v1, QueryNewsListStructure v2) throws Exception {
        Map<String, List<RankedResult>> queryListMap1 = v1.getQueryListMap();
        Map<String, List<RankedResult>> queryListMap2 = v2.getQueryListMap();

        Map<String, List<RankedResult>> newMap = new HashMap<>();

        Set<String> queries = queryListMap1.keySet();

        for (String query: queries){
            List<RankedResult> join = new ArrayList<>();

            join.addAll(queryListMap1.get(query));
            join.addAll(queryListMap2.get(query));
            newMap.put(query,join);
        }




        return new QueryNewsListStructure(newMap);
    }
}
