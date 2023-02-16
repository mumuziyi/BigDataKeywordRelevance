package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.*;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleCount;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.MyDPHMergeStructure;

import java.util.*;

public class MyFunctions {
    String newsPath;
    String queryPath;
    SparkSession spark;

    public MyFunctions(String newsPath, String queryPath, SparkSession spark) {
        this.newsPath = newsPath;
        this.queryPath = queryPath;
        this.spark = spark;
    }

    public void process() {
        Dataset<Row> queryFilesAsRowTable = spark.read().text(queryPath);
        Dataset<Row> newsFileAsRowTable = spark.read().text(newsPath);

        Dataset<Query> queryDataset = queryFilesAsRowTable.map(new QueryFormaterMap(), Encoders.bean(Query.class));
        Dataset<NewsArticle> newsArticleDataset = newsFileAsRowTable.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class));

        Dataset<NewsArticle> processedArticleDataset = newsArticleDataset.map(new ProcessNewsArticle(), Encoders.bean(NewsArticle.class));
        Dataset<Query> processedQueryDataset = queryDataset.map(new ProcessQuery(), Encoders.bean(Query.class));

//        LongAccumulator wordCountAccumulator = spark.sparkContext().longAccumulator();
        //计算所有文件的总字符数
        LongAccumulator fileCountAccumulator = spark.sparkContext().longAccumulator();
        //记录所有文件的当前term数量
        LongAccumulator termCountInAllDocument = spark.sparkContext().longAccumulator();

        List<Query> queries = processedQueryDataset.collectAsList();


        // 对于每一个query进行计算
        for (Query query : queries) {

//            Broadcast<Query> queryBroadcast = spark.sparkContext().broadcast(query, classTag);
            StringBuilder queryRecord = new StringBuilder();

            List<String> terms = query.getQueryTerms();
            // 存放每一次的结果，当这个query结束时合并结果
            Broadcast<List<String>> termsBroadcast = spark.sparkContext().broadcast(terms, scala.reflect.ClassTag$.MODULE$.apply(List.class));
            List<Dataset<ArticleDPHScore>> entireQueryDPH = new ArrayList<>();

            for (String term : termsBroadcast.value()) {
                queryRecord.append(term + "  ");
                // 分别为，newsArticle，当前文件的长度，当前文件term的数量
                Dataset<ArticleCount> articleCountTuple =
                        processedArticleDataset.map(new WordCountMap(fileCountAccumulator, termCountInAllDocument, term),
                                Encoders.bean(ArticleCount.class));
                articleCountTuple.count();

                long fileCountAll = fileCountAccumulator.value();
                long termCountAll = termCountInAllDocument.value();
//
                Dataset<ArticleDPHScore> articlesDPHScores = articleCountTuple.map(
                        new DPHCalculateMap(fileCountAll, termCountAll, articleCountTuple.count()),
                        Encoders.bean(ArticleDPHScore.class));

                articlesDPHScores.count();

                entireQueryDPH.add(articlesDPHScores);

            System.out.println("Finish this query");
            List<MyDPHMergeStructure> mergedList = mergeDPHScoreList(entireQueryDPH);
            for (MyDPHMergeStructure structure: mergedList){
                if (!structure.getNews().getTitle().equals("") && structure.getScore() > 0){
                    System.out.println(queryRecord.toString() + ": " + structure.getNews().getTitle() + "   " + structure.getScore());
                }
            }
            System.out.println(fileCountAccumulator.value());
            }
        }
    }
//}

    public List<MyDPHMergeStructure> mergeDPHScoreList(List<Dataset<ArticleDPHScore>> DPHScores){

        Map<NewsArticle,Double> mergeMap = new HashMap<>();

        int length = DPHScores.size();

        for (Dataset<ArticleDPHScore> tupleList: DPHScores){
            tupleList.foreach(tuple -> {
                if (tuple.DPHScore > 0 ) {
                    mergeMap.put(tuple.Article,mergeMap.getOrDefault(tuple.Article, 0.0) + tuple.DPHScore);
                }else {
                    mergeMap.put(tuple.Article,mergeMap.getOrDefault(tuple.Article, 0.0) + 0);
                }
            });
        }

        List<MyDPHMergeStructure> mergedList = new ArrayList<>();
        Set<NewsArticle> keySets = mergeMap.keySet();
        for (NewsArticle newsArticle: keySets ){
            mergedList.add(new MyDPHMergeStructure(newsArticle,mergeMap.get(newsArticle)/length));
        }

        return mergedList;

    }
}
