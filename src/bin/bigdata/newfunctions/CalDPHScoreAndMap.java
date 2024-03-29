package bin.bigdata.newfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import bin.bigdata.newstructures.NewsCount;
import bin.bigdata.newstructures.NewsDPHScore;
import bin.bigdata.providedstructures.Query;
import bin.bigdata.providedutilities.DPHScorer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class is used to Calculate DPH Score
 * MapFunction convert (news, allKindsOfCount) Pair to (news, List(Query,DPHScore))
 *
 */

public class CalDPHScoreAndMap implements MapFunction<NewsCount, NewsDPHScore> {
    Broadcast<List<Query>> broadcastQuery;
    Map<String, Long> accumulatorMap;
    Long totalLengthInAll;

    Long newsNumberInAll;

    public CalDPHScoreAndMap(Broadcast<List<Query>> broadcastQuery, Map<String, Long> accumulatorMap, Long totalLength, Long newsNumberInAll) {
        this.broadcastQuery = broadcastQuery;
        this.accumulatorMap = accumulatorMap;
        this.totalLengthInAll = totalLength;
        this.newsNumberInAll = newsNumberInAll;
    }

    @Override
    public NewsDPHScore call(NewsCount value) throws Exception {

        Map<String, Double> queryDoubleMap = new HashMap<>();
//        Retrieve the query list from the broadcast variable.
        List<Query> queries = broadcastQuery.value();
//        TermCountMap that tracks the count of each query's term in the news article.
        Map<String, Integer> termFreqInCur = value.getTermCountMap();

        for (Query query : queries) {
            double score = 0.0;
            List<String> terms = query.getQueryTerms();
            for (String term : terms) {
//                Get the occurrence of THE TERM in ALL articles.
                long termOccurrence = accumulatorMap.get(term);
                if (!termFreqInCur.containsKey(term) || value.getTotalLength() == 0) {
                    continue;
                }
                int temp1 = termFreqInCur.get(term);

                score += DPHScorer.getDPHScore((short) temp1, (int) termOccurrence, value.getTotalLength(),
                        totalLengthInAll / newsNumberInAll, newsNumberInAll);
            }
            queryDoubleMap.put(query.getOriginalQuery(), score / query.getQueryTerms().size());

        }

        return new NewsDPHScore(value.getNewsArticle(), queryDoubleMap);
    }
}
