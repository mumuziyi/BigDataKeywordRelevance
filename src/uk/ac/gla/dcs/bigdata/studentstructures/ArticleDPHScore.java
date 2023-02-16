package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class ArticleDPHScore {
    public NewsArticle Article;
    public double DPHScore;

    public ArticleDPHScore(NewsArticle article, double DPHScore) {
        Article = article;
        this.DPHScore = DPHScore;
    }
}
