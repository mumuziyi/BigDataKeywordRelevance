package uk.ac.gla.dcs.bigdata.studentstructures;

import org.jetbrains.annotations.NotNull;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.util.Objects;

public class MyDPHMergeStructure implements Comparable{

    // 出现次数，用于求平均值

    String string;

    double score;

    public MyDPHMergeStructure(String string, double score) {
        this.string = string;
        this.score = score;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }


    @Override
    public int compareTo(@NotNull Object o) {
        if (o instanceof MyDPHMergeStructure){
            MyDPHMergeStructure anotherStructure = (MyDPHMergeStructure)o;
            return this.score > anotherStructure.getScore()? -1: 1;
        }
        return -1;
    }
}
