package uk.ac.gla.dcs.bigdata.MyMaps;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.MyStructure.NewsCount;
import uk.ac.gla.dcs.bigdata.MyStructure.NewsDPHScore;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CalDPHScoreAndMap implements MapFunction<NewsCount, NewsDPHScore> {
    Broadcast<List<Query>> broadcastQuery;
    Map<String, Long> accumulatorMap;
    Long totalLengthInAll;

    Long newsNumberInAll;


    /**
     * @param broadcastQuery A list of Query objects that are broadcast to all nodes.
     * @param accumulatorMap A map that keeps track of each query term's appearances in ALL articles.
     * @param totalLength A fixed global length of all the articles.
     * @param newsNumberInAll A fixed global number of all articles.
     */
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
