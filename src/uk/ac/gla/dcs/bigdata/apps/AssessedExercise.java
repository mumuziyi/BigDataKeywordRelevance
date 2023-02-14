package uk.ac.gla.dcs.bigdata.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsTransformation;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsWordProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.ProcessNewsArticle;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.ProcessQuery;
import uk.ac.gla.dcs.bigdata.studentstructures.ContentEssential;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsEssential;

import java.io.File;
import java.util.List;

/**
 * This is the main class where your Spark topology should be specified.
 * <p>
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 *
 * @author Richard
 */
public class AssessedExercise {


    public static void main(String[] args) {

        File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
        System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

        // The code submitted for the assessed exerise may be run in either local or remote modes
        // Configuration of this will be performed based on an environment variable
        String sparkMasterDef = System.getenv("spark.master");
        if (sparkMasterDef == null) sparkMasterDef = "local[2]"; // default is local mode with two executors

        String sparkSessionName = "BigDataAE"; // give the session a name

        // Create the Spark Configuration
        SparkConf conf = new SparkConf()
                                 .setMaster(sparkMasterDef)
                                 .setAppName(sparkSessionName);

        // Create the spark session
        SparkSession spark = SparkSession
                                     .builder()
                                     .config(conf)
                                     .getOrCreate();


        // Get the location of the input queries
        String queryFile = System.getenv("bigdata.queries");
        if (queryFile == null) queryFile = "data/queries.list"; // default is a sample with 3 queries

        // Get the location of the input news articles
        String newsFile = System.getenv("bigdata.news");
        if (newsFile == null)
            newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles

        Dataset<Row> queryFilesAsRowTable = spark.read().text(queryFile);
        Dataset<Row> newsFileAsRowTable = spark.read().text(newsFile);

        Dataset<Query> queryDataset = queryFilesAsRowTable.map(new QueryFormaterMap(),Encoders.bean(Query.class));
        Dataset<NewsArticle> newsArticleDataset = newsFileAsRowTable.map(new NewsFormaterMap(),Encoders.bean(NewsArticle.class));

        Dataset<NewsArticle> processedArticleDataset = newsArticleDataset.map(new ProcessNewsArticle(),Encoders.bean(NewsArticle.class));
        Dataset<Query> processedQueryDataset = queryDataset.map(new ProcessQuery(),Encoders.bean(Query.class));

        List<NewsArticle> list = processedArticleDataset.collectAsList();

        for (NewsArticle newsArticle: list){
            System.out.println(newsArticle.getContents().get(0).getContent());
        }



        // Call the student's code
//        List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

        // Close the spark session
        spark.close();

        // Check if the code returned any results
//        if (results == null)
//            System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
//        else {
//
//            // We have set of output rankings, lets write to disk
//
//            // Create a new folder
//            File outDirectory = new File("results/" + System.currentTimeMillis());
//            if (!outDirectory.exists()) outDirectory.mkdir();
//
//            // Write the ranking for each query as a new file
//            for (DocumentRanking rankingForQuery : results) {
//                rankingForQuery.write(outDirectory.getAbsolutePath());
//            }
//        }


    }


    public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

        // Load queries and news articles
        Dataset<Row> queriesjson = spark.read().text(queryFile);
        Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

        // Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
        Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
        Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

        //----------------------------------------------------------------
        // Your Spark Topology should be defined here
        //----------------------------------------------------------------
        TextPreProcessor preProcessor = new TextPreProcessor();

//        Map each news article to a NewsEssential object, store them in a new Dataset
        Dataset<NewsEssential> newsEssentialDataset = news.map(new NewsTransformation(), Encoders.bean(NewsEssential.class));
//        Removing all stop words and stemming for each news article and each query
        Dataset<NewsEssential> processedNewsDataset = newsEssentialDataset.map(new NewsWordProcessor(), Encoders.bean(NewsEssential.class));
        Dataset<Query> processedQueries = queries.map(new QueryProcessor(), Encoders.bean(Query.class));

//        Testing
        processedNewsDataset.foreach((ForeachFunction<NewsEssential>) newsEssential -> {
            System.out.println(newsEssential.getContents().get(0).getContent());
        });
        processedQueries.foreach((ForeachFunction<Query>) query -> {
            System.out.println(query.getOriginalQuery());
        });

        return null; // replace this with the the list of DocumentRanking output by your topology
    }


}
