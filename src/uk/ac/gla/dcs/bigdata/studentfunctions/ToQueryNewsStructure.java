package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryNewsListStructure;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

import java.util.*;

/**
 * This class is used to Convert (news,scoreList) to (Query, NewsList), and use this kind of date to reduce
 */

public class ToQueryNewsStructure implements MapFunction<NewsDPHScore, QueryNewsListStructure> {

    @Override
    public QueryNewsListStructure call(NewsDPHScore value) throws Exception {
        // 变为query List，方便后续进行reduce
        Map<String, List<RankedResult>> ans = new HashMap<>();

        Map<String,Double> queryDoubleMap = value.getQueryDoubleMap();

        Set<String> queries = queryDoubleMap.keySet();

        for (String query: queries){
            List<RankedResult> cur = new ArrayList<>();
            if (queryDoubleMap.get(query) != 0){
                cur.add(new RankedResult(value.getNewsArticle().getId(),value.getNewsArticle(),queryDoubleMap.get(query)));
            }

            ans.put(query, cur);
        }

        return new QueryNewsListStructure(ans);
    }
}
