package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class MyDPHMergeStructure {

    NewsArticle news;
    // 出现次数，用于求平均值

    double score;

    public MyDPHMergeStructure(NewsArticle news, double score) {
        this.news = news;

        this.score = score;
    }

    public NewsArticle getNews() {
        return news;
    }

    public void setNews(NewsArticle news) {
        this.news = news;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
