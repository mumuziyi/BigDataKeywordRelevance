package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.util.Objects;

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

    @Override
    public boolean equals(Object another){
        //先判断是不是自己,提高运行效率
        if (this == another)
            return true;

        //再判断是不是Person类,提高代码的健壮性
        if (another instanceof MyDPHMergeStructure) {

            //向下转型,父类无法调用子类的成员和方法
            MyDPHMergeStructure anotherPerson = (MyDPHMergeStructure) another;

            //最后判断类的所有属性是否相等，其中String类型和Object类型可以用相应的equals()来判断
            if (this.getNews().getTitle().equals(anotherPerson.getNews().getTitle()))
                return true;
        } else {
            return false;
        }

        return false;
    }
    @Override
    public int hashCode(){
        return this.news.getTitle().hashCode();
    }
}
