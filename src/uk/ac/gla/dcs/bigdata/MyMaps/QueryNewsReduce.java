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
        Map<Query, List<RankedResult>> queryListMap1 = v1.getQueryListMap();
        Map<Query, List<RankedResult>> queryListMap2 = v2.getQueryListMap();

        Map<Query, List<RankedResult>> newMap = new HashMap<>();

        Set<Query> queries = queryListMap1.keySet();

        for (Query query:queries){
//            System.out.println(queryListMap1.get(query).size());
            List<RankedResult> join = new ArrayList<>();
            join.addAll(queryListMap1.get(query));

            if (queryListMap2.get(query) != null){
                join.addAll(queryListMap2.get(query));
            }


            newMap.put(query,join);
//            queryListMap1.get(query).addAll(queryListMap2.get(query));
//            newMap.put(query,queryListMap1.get(query));
//
//            System.out.println(queryListMap1.get(query).size() + "  " + queryListMap2.get(query).size());
//            // 都不为空，合并两个列表
//            if (queryListMap1.get(query) != null && queryListMap2.get(query) != null){
//                queryListMap1.get(query).addAll(queryListMap2.get(query));
//                newMap.put(query,queryListMap1.get(query));
//
//            }else if (queryListMap1.get(query) == null){// 1空，2不空返回2，否则返回null
//                if (queryListMap2.get(query) != null){
//                    newMap.put(query,queryListMap2.get(query));
//                }else {
//                    newMap.put(query,null);
//                }
//            }else {//2 空 1不空
//                newMap.put(query,queryListMap1.get(query));
//            }

        }

        return new QueryNewsListStructure(newMap);
    }
}
