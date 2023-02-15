package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.internal.Trees;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.DPHCalculateMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.ProcessNewsArticle;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.ProcessQuery;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.WordCountMap;

import javax.xml.crypto.Data;
import java.util.List;

public class MyFunctions {
    String newsPath;
    String queryPath;
    SparkSession spark;

    public MyFunctions(String newsPath, String queryPath, SparkSession spark){
        this.newsPath = newsPath;
        this.queryPath = queryPath;
        this.spark = spark;
    }

    public void process(){
        Dataset<Row> queryFilesAsRowTable = spark.read().text(queryPath);
        Dataset<Row> newsFileAsRowTable = spark.read().text(newsPath);

        Dataset<Query> queryDataset = queryFilesAsRowTable.map(new QueryFormaterMap(), Encoders.bean(Query.class));
        Dataset<NewsArticle> newsArticleDataset = newsFileAsRowTable.map(new NewsFormaterMap(),Encoders.bean(NewsArticle.class));

        Dataset<NewsArticle> processedArticleDataset = newsArticleDataset.map(new ProcessNewsArticle(),Encoders.bean(NewsArticle.class));
        Dataset<Query> processedQueryDataset = queryDataset.map(new ProcessQuery(),Encoders.bean(Query.class));

//        LongAccumulator wordCountAccumulator = spark.sparkContext().longAccumulator();
        //计算所有文件的总字符数
        LongAccumulator fileCountAccumulator = spark.sparkContext().longAccumulator();
        //记录所有文件的当前term数量
        LongAccumulator termCountInAllDocument = spark.sparkContext().longAccumulator();


        // 分别为，newsArticle，当前文件的长度，当前文件term的数量
        Dataset<Tuple3<NewsArticle,Integer,Short>> articleCountTuple =
                processedArticleDataset.map(new WordCountMap(fileCountAccumulator,termCountInAllDocument,"facebook"),
                        Encoders.tuple(Encoders.bean(NewsArticle.class),Encoders.INT(),Encoders.SHORT()));
        articleCountTuple.count();

        System.out.println("===========================");


        List<Tuple3<NewsArticle,Integer,Short>> articles = articleCountTuple.collectAsList();

        for (Tuple3<NewsArticle,Integer,Short> tuple3 : articles){
            if (tuple3._3() != 0){
                System.out.println(tuple3._3());
            }
        }
//

//        Dataset<Tuple2<NewsArticle,Double>> articleDPHScore =
//                articleCountTuple.map(new DPHCalculateMap(fileCountAccumulator,
//                        termCountInAllDocument,processedArticleDataset.count()),
//                        Encoders.tuple(Encoders.bean(NewsArticle.class),Encoders.DOUBLE()));



//        StringBuilder sb = new StringBuilder();
//
//        Tuple3<NewsArticle,Integer,Short> test = articles.get(0);
//        double averageLength = fileCountAccumulator.value()/processedArticleDataset.count();
//        System.out.println("finish ++++++++++ ");
//        Double DPHScore = DPHScorer.getDPHScore(test._3(), Math.toIntExact(termCountInAllDocument.value()),test._2(),
//                averageLength,processedArticleDataset.count());

//        System.out.println(DPHScore);
        List<Query> queries = processedQueryDataset.collectAsList();
        for (Query query : queries){
            List<String> terms = query.getQueryTerms();
            for (String term: terms){
                System.out.println(term);

//                // 分别为，newsArticle，当前文件的长度，当前文件term的数量
//                Dataset<Tuple3<NewsArticle,Integer,Short>> articleCountTuple =
//                        processedArticleDataset.map(new WordCountMap(fileCountAccumulator,termCountInAllDocument,"financ"),
//                                Encoders.tuple(Encoders.bean(NewsArticle.class),Encoders.INT(),Encoders.SHORT()));
//
//                List<Tuple3<NewsArticle,Integer,Short>> articles = articleCountTuple.collectAsList();
//
//                StringBuilder sb = new StringBuilder();
//                for (Tuple3<NewsArticle,Integer,Short> test:articles){
//                    Double DPHScore = DPHScorer.getDPHScore(test._3(), Math.toIntExact(termCountInAllDocument.value()),test._2(),
//                            fileCountAccumulator.value()/processedArticleDataset.count(),processedArticleDataset.count());
//
//                    sb.append(DPHScore + "   ");
//                }
//                System.out.println(sb.toString() + "---------------------");

//                Tuple3<NewsArticle,Integer,Short> test = articles.get(0);
//                System.out.println("term is " + term);
//                System.out.println("term count" + test._3());
//                System.out.println("total term count " + Math.toIntExact(termCountInAllDocument.value()));
//                System.out.println("file count " + test._2());
//                System.out.println("total file count " + fileCountAccumulator.value() + "  fileCountAccumulator.value()/processedArticleDataset.count() ");
//                System.out.println("document count" + processedArticleDataset.count());
//
//                Double DPHScore = DPHScorer.getDPHScore(test._3(), Math.toIntExact(termCountInAllDocument.value()),test._2(),
//                        fileCountAccumulator.value()/processedArticleDataset.count(),processedArticleDataset.count());
//                System.out.println("DPH " + DPHScore);
//
//
//                System.out.println(term + " +++++++++++++++++ ");


            }
        }


        System.out.println(fileCountAccumulator.value());




//        List<Tuple2<NewsArticle,Long>> list = articleCountTuple.collectAsList();
//
//
//        for (Tuple2<NewsArticle,Long> tuple2 : list ){
////            System.out.println(tuple2._1 + "  " + tuple2._1.getContents().toString());
//            System.out.println(tuple2._1.getTitle() + "   " + tuple2._2);
//        }

//        System.out.println(fileCountAccumulator.value()/articleCountTuple.count());


    }
}
