package uk.ac.gla.dcs.bigdata.MyMaps;

import org.apache.spark.util.LongAccumulator;

public class TermAccumulator {
    String term;
    LongAccumulator accumulator;

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public LongAccumulator getAccumulator() {
        return accumulator;
    }

    public void setAccumulator(LongAccumulator accumulator) {
        this.accumulator = accumulator;
    }

    public TermAccumulator() {
    }

    public TermAccumulator(String term, LongAccumulator accumulator) {
        this.term = term;
        this.accumulator = accumulator;
    }
}
