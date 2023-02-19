package uk.ac.gla.dcs.bigdata.MyMaps;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.sources.In;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.MyStructure.NewsCount;
import uk.ac.gla.dcs.bigdata.MyStructure.NewsDPHScore;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

import java.security.Key;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CalDPHScoreAndMap implements MapFunction<NewsCount, NewsDPHScore> {
    Broadcast<List<Query>> broadcastQuery;
    Map<String, LongAccumulator> accumulatorMap;
    LongAccumulator totalLengthInAll;

    LongAccumulator newsNumberInAll;


    public CalDPHScoreAndMap(Broadcast<List<Query>> broadcastQuery, Map<String, LongAccumulator> accumulatorMap, LongAccumulator totalLength,LongAccumulator newsNumberInAll) {
        this.broadcastQuery = broadcastQuery;
        this.accumulatorMap = accumulatorMap;
        this.totalLengthInAll = totalLength;
        this.newsNumberInAll = newsNumberInAll;
    }

    @Override
    public NewsDPHScore call(NewsCount value) throws Exception {

        Map<Query,Double> queryDoubleMap = new HashMap<>();

        List<Query> queries = broadcastQuery.value();

        Map<String, Integer> termFreqInCur = value.getTermCountMap();

//        Set<String> keys = termFreqInCur.keySet();
//        for (String key:keys){
//            System.out.println("+++++" + key+ "  " +termFreqInCur.get(key));
//            System.out.println(termFreqInCur.containsKey("financ"));
//        }

        for (Query query: queries){
            double score = 0.0;
            List<String> terms = query.getQueryTerms();
            for (String term: terms){
                long temp2 = accumulatorMap.get(term).value();
                if (!termFreqInCur.containsKey(term) || value.getTotalLength() == 0){
                    continue;
                }
                int temp1 = termFreqInCur.get(term);

                score += DPHScorer.getDPHScore((short) temp1,(int) temp2,
                        value.getTotalLength(),totalLengthInAll.value()/newsNumberInAll.value(),
                        newsNumberInAll.value());
            }
            queryDoubleMap.put(query,score/query.getQueryTerms().size());

            if (score/query.getQueryTerms().size()!= 0){
                System.out.println(query.getOriginalQuery()+ "  " +value.getNewsArticle().getTitle() + "  "  + score/query.getQueryTerms().size());
            }
        }

        return null;
    }
}
