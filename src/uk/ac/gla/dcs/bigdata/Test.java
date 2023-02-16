//package uk.ac.gla.dcs.bigdata;
//
//import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
//import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
//import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
//import uk.ac.gla.dcs.bigdata.studentstructures.MyDPHMergeStructure;
//
//import java.util.*;
//
//public class Test {
//    // public NewsArticle(String id, String article_url, String title, String author, long published_date,
//    //			List<ContentItem> contents, String type, String source) {
//    public static void main(String[] args) {
//        List<ContentItem> test1 = new ArrayList<>();
//        NewsArticle article1 = new NewsArticle("1234","qwer","adsfa","sadfasdf",123412, test1,"dafasd","asdfwef");
//
//        NewsArticle article2 = new NewsArticle("1234","qasdfa","adsfa","sadfasdf",123412, test1,"dafasd","asdfwef");
//
//        MyDPHMergeStructure structure1 = new MyDPHMergeStructure(article1,30.0);
//        MyDPHMergeStructure structure2 = new MyDPHMergeStructure(article2,30.0);
//
//        System.out.println(structure1.equals(structure2));
//
//        Set<NewsArticle> set = new HashSet<>();
//
//        set.add(article1);
//        System.out.println(set.contains(article2));
//
//
//    }
//}
