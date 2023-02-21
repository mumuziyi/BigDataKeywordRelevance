package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

import java.util.*;
/**
 * This class contains some functions needed in the process
 */


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

    public static List<RankedResult> getTop10(List<RankedResult> resultList){
        List<RankedResult> ans = new ArrayList<>();
        for (RankedResult result: resultList){
            // 第一个直接加进去
            if (ans.size() == 0){
                ans.add(result);
            }

            boolean lessThan05 = false;

            //遍历所有list中的数据，保如果距离小于0.5 舍弃,进入下一个result
            for (RankedResult cur: ans){
                if (TextDistanceCalculator.similarity(cur.getArticle().getTitle(),result.getArticle().getTitle()) < 0.5){
                    lessThan05 = true;
                    break;
                }
            }

            // 当前title与list中所有title的距离都大于0.5，加入当前list
            if (!lessThan05){
                ans.add(result);
            }

            // 只要前十个
            if (ans.size() >= 10){
                break;
            }

        }
        return ans;

    }
}
