package uk.ac.gla.dcs.bigdata.MyMaps;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.MyStructure.NewsDPHScore;
import uk.ac.gla.dcs.bigdata.MyStructure.QueryNewsListStructure;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

import java.util.*;

public class ToQueryNewsStructure implements MapFunction<NewsDPHScore, QueryNewsListStructure> {

    @Override
    public QueryNewsListStructure call(NewsDPHScore value) throws Exception {
        // 变为query List，方便后续进行reduce
        Map<Query, List<RankedResult>> ans = new HashMap<>();

        Map<Query,Double> queryDoubleMap = value.getQueryDoubleMap();

        Set<Query> queries = queryDoubleMap.keySet();

        for (Query query: queries){
            List<RankedResult> cur = new ArrayList<>();
            cur.add(new RankedResult(value.getNewsArticle().getId(),value.getNewsArticle(),queryDoubleMap.get(query)));
            ans.put(query, cur);
        }

        return new QueryNewsListStructure(ans);
    }
}
