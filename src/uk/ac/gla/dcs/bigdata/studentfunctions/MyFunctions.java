package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.MAP;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;
import scala.reflect.internal.Trees;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.*;
import uk.ac.gla.dcs.bigdata.studentstructures.MyDPHMergeStructure;

import javax.xml.crypto.Data;
import java.util.*;

public class MyFunctions {
    String newsPath;
    String queryPath;
    SparkSession spark;

    public MyFunctions(String newsPath, String queryPath, SparkSession spark){
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

//        Broadcast the processedQueryDataset to all the nodes
        ClassTag<Dataset<Query>> classTag = scala.reflect.ClassTag$.MODULE$.apply(Dataset.class);
        Broadcast<Dataset<Query>> br = spark.sparkContext().broadcast(processedQueryDataset, classTag);


        // 对于每一个query进行计算
        for (Query query : queries) {
            StringBuilder queryRecord = new StringBuilder();

            List<String> terms = query.getQueryTerms();
            // 存放每一次的结果，当这个query结束时合并结果
            List<List<Tuple2<String, Double>>> entireQueryDPH = new ArrayList<>();

            for (String term : terms) {
                queryRecord.append(term + "  ");
                // 分别为，newsArticle，当前文件的长度，当前文件term的数量，这些东西jiang'bei
                Dataset<Tuple3<String, Integer, Short>> articleCountTuple =
                        processedArticleDataset.map(new WordCountMap(fileCountAccumulator, termCountInAllDocument, term),
                                Encoders.tuple(Encoders.STRING(), Encoders.INT(), Encoders.SHORT()));
                articleCountTuple.count();

                long fileCountAll = fileCountAccumulator.value();
                long termCountAll = termCountInAllDocument.value();

                Dataset<Tuple2<String, Double>> articlesDPHScores = articleCountTuple.map(
                        new DPHCalculateMap(fileCountAll, termCountAll, articleCountTuple.count()),
                        Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()));

                // Print the DPH score
                List<Tuple2<String, Double>> tuple2s = articlesDPHScores.collectAsList();


                entireQueryDPH.add(tuple2s);

            }
            System.out.println("Finish this query");
            List<MyDPHMergeStructure> mergedList = mergeDPHScoreList(entireQueryDPH);
            Collections.sort(mergedList);
            System.out.println(mergedList.size());
//            From the first one in list, read every one of the list, compare it with every on in the new list, if their distance is over 0.5, then add it into the list
//            Create a linkedlist of size 10
            List<MyDPHMergeStructure> finalList = new ArrayList<>(10);
            for(int i = 0; i <= mergedList.size(); i++){
                if(finalList.size() == 0){
                    finalList.add(mergedList.get(i));
                }else if(finalList.size() < 10){
                    for (int j = 0; j < finalList.size(); j++) {
                        MyDPHMergeStructure finalStructure = finalList.get(j);
                        if (TextDistanceCalculator.similarity(mergedList.get(i).getString(), finalStructure.getString()) <= 0.5) {
                            break;
                        }
                    }
                    finalList.add(mergedList.get(i));
                }
            }
            System.out.println("Final list size: " + finalList.size());

//            Strip the final list to top 10
            List<MyDPHMergeStructure> top10List = finalList.subList(0, 10);
            System.out.println("Final list size: " + top10List.size());
            for (MyDPHMergeStructure structure : top10List) {
                System.out.println(queryRecord + ": " + structure.getString() + "   " + structure.getScore());
            }
        }
    }


    public List<MyDPHMergeStructure> mergeDPHScoreList(List<List<Tuple2<String,Double>>> DPHScores){

        Map<String,Double> mergeMap = new HashMap<>();

        int length = DPHScores.size();

        for (List<Tuple2<String,Double>> tupleList: DPHScores){
            for (Tuple2<String,Double> tuple: tupleList){
                if (tuple._2() > 0 ) {
                    mergeMap.put(tuple._1(),mergeMap.getOrDefault(tuple._1(), 0.0) + tuple._2());
                }
//                else {
//                    mergeMap.put(tuple._1(),mergeMap.getOrDefault(tuple._1(), 0.0) + 0);
//                }

            }
        }

        List<MyDPHMergeStructure> mergedList = new ArrayList<>();
        Set<String> keySets = mergeMap.keySet();
        for (String str: keySets ){
            mergedList.add(new MyDPHMergeStructure(str,mergeMap.get(str)/length));
        }

        return mergedList;

    }
}
