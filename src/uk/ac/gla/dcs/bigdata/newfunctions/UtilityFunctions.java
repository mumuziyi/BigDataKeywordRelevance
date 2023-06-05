package uk.ac.gla.dcs.bigdata.newfunctions;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

import java.util.*;
/**
 * This class contains two utility functions that are used in the whole project.
 */
public class UtilityFunctions {
    public static Set<String> getTermsSet(List<Query> list){
        Set<String> set = new HashSet<>();
        for (Query query: list){
            List<String> terms = query.getQueryTerms();
            set.addAll(terms);
        }
        return set;
    }

    /**
     * This generates a global map of query terms and their corresponding accumulators. This is used in recording
     * each term's occurrence across the whole dataset.
     * @param set Set of query terms in all queries.
     * @param spark The SparkSession, it's required to create new global accumulators.
     * @return A map of query terms and their corresponding accumulators.
     */
    public static Map<String, LongAccumulator> getAccumulator(Set<String> set, SparkSession spark){
        Map<String, LongAccumulator> accumulatorMap = new HashMap<>();
        for (String str: set){
            accumulatorMap.put(str,spark.sparkContext().longAccumulator());
        }
        return accumulatorMap;
    }

    /**
     * This function takes in the raw yet sorted list of {@link RankedResult}, and return the top 10. This
     * process also makes sure the top 10's title has distance over 0.5 between each other.
     * @param resultList The raw list of RankedResult
     * @return A list of RankedResult with size 10, containing the tops.
     */
    public static List<RankedResult> getTop10(List<RankedResult> resultList){
        List<RankedResult> ans = new ArrayList<>();
        for (RankedResult result: resultList){
//            No need to calculate the first element's distance because there's no other element yet.
            if (ans.size() == 0){
                ans.add(result);
            }

            boolean lessThan05 = false;

//            For all the existing title, try to calculate the distance between the current title.
            for (RankedResult cur: ans){
                if (TextDistanceCalculator.similarity(cur.getArticle().getTitle(),result.getArticle().getTitle()) < 0.5){
                    lessThan05 = true;
                    break;
                }
            }
//            If the current title's distance between any existing title in the list is greater than 0.5,
//            add it to the list.
            if (!lessThan05){
                ans.add(result);
            }

//            We only need the first 10 elements.
            if (ans.size() >= 10){
                break;
            }

        }
        return ans;

    }
}
