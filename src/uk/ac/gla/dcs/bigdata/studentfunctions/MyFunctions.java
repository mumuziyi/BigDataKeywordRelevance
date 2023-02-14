package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.reflect.internal.Trees;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.ProcessNewsArticle;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.ProcessQuery;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.WordCountMap;

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
        LongAccumulator fileCountAccumulator = spark.sparkContext().longAccumulator();


        Dataset<Tuple2<NewsArticle,Long>> articleCountTuple =
                processedArticleDataset.map(new WordCountMap(fileCountAccumulator), Encoders.tuple(Encoders.bean(NewsArticle.class),Encoders.LONG()));

        List<Tuple2<NewsArticle,Long>> list = articleCountTuple.collectAsList();

        for (Tuple2<NewsArticle,Long> tuple2 : list ){
//            System.out.println(tuple2._1 + "  " + tuple2._1.getContents().toString());
            System.out.println(tuple2._1.getTitle() + "   " + tuple2._2);
        }

//
//        List<ContentItem> first = processedArticleDataset.collectAsList().get(0).getContents();
//        for (ContentItem contentItem: first){
//            System.out.println(contentItem.getContent());
//        }

        System.out.println(fileCountAccumulator.value()/articleCountTuple.count());


    }
}
