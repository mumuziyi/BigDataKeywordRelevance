package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;
import org.roaringbitmap.art.Art;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.internal.Trees;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleCount;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleDPHScore;

import java.security.PublicKey;

public class DPHCalculateMap implements MapFunction<ArticleCount, ArticleDPHScore> {

    long fillCountAll;
    long termCountAll;

    long count;
    public DPHCalculateMap(long fileCountAll, long termCountAll, long count){
        this.fillCountAll = fileCountAll;
        this.termCountAll = termCountAll;
        this.count = count;

    }

    @Override
//    public Tuple2<NewsArticle, Double> call(Tuple3<NewsArticle, Integer, Short> value) throws Exception {
        public ArticleDPHScore call(ArticleCount value) throws Exception {
//        Tuple3<NewsArticle, Integer, Short> test = value;
        ArticleCount test = value;
        Double DPHScore = 0.0;

        double averageLength = (double)fillCountAll/count;
        if (test.ArticleLength == 0){

        }else {
             DPHScore = DPHScorer.getDPHScore(test.TermCount, Math.toIntExact(termCountAll),test.ArticleLength,
                    averageLength,count);
        }

        return new ArticleDPHScore(test.Article,DPHScore);
    }
}
