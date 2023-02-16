package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class ArticleDPHScore {
    public NewsArticle Article;
    public double DPHScore;

    public ArticleDPHScore(NewsArticle article, double DphScore) {
        Article = article;
        if(Double.isNaN(DphScore)){
            DPHScore = 0.0;
        }else{
            DPHScore = DphScore;
        }
    }
}
