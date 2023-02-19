package uk.ac.gla.dcs.bigdata.MyMaps;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.util.*;


public class MyFunctions {
    public static Set<String> getTermsSet(List<Query> list){
        Set<String> set = new HashSet<>();
        for (Query query: list){
            List<String> terms = query.getQueryTerms();
            set.addAll(terms);
        }
        return set;
    }

    public static Map<String, LongAccumulator> getAccumulator(Set<String> set, SparkSession spark){
        Map<String, LongAccumulator> accumulatorMap = new HashMap<>();
        for (String str: set){
            accumulatorMap.put(str,spark.sparkContext().longAccumulator());
        }
        return accumulatorMap;
    }
}
