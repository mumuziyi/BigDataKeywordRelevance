package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import scala.Tuple2;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

public class DPHCalculateMap implements MapFunction<Tuple3<String, Integer, Short>, Tuple2<String, Double>> {

    long fillCountAll;
    long termCountAll;
    long count;

    public DPHCalculateMap(long fileCountAll, long termCountAll, long count) {
        this.fillCountAll = fileCountAll;
        this.termCountAll = termCountAll;
        this.count = count;
    }

    @Override
    public Tuple2<String, Double> call(Tuple3<String, Integer, Short> value) throws Exception {
        double DPHScore = 0.0;

        double averageLength = (double) fillCountAll / count;
        if (value._2() != 0) {
            DPHScore = DPHScorer.getDPHScore(value._3(), Math.toIntExact(termCountAll), value._2(),
                    averageLength, count);
        }

        return new Tuple2<>(value._1(), DPHScore);
    }

}
