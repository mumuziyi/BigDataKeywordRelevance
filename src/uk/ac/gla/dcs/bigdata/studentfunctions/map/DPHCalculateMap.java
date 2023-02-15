package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.internal.Trees;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

import java.security.PublicKey;

public class DPHCalculateMap implements MapFunction<Tuple3<NewsArticle,Integer,Short>,Tuple2<NewsArticle,Double>> {

    LongAccumulator termCountInAllDocument;
    LongAccumulator fileCountAccumulator;

    long count;
    public DPHCalculateMap(LongAccumulator fileCountAccumulator, LongAccumulator termCountInAllDocument, long count){
        this.fileCountAccumulator = fileCountAccumulator;
        this.termCountInAllDocument = termCountInAllDocument;
        this.count = count;

    }

    @Override
    public Tuple2<NewsArticle, Double> call(Tuple3<NewsArticle, Integer, Short> value) throws Exception {
        Tuple3<NewsArticle, Integer, Short> test = value;

        double averageLength = fileCountAccumulator.value()/count;

        Double DPHScore = DPHScorer.getDPHScore(test._3(), Math.toIntExact(termCountInAllDocument.value()),test._2(),
                averageLength,count);

        return new Tuple2<>(value._1(),DPHScore);
    }
}
