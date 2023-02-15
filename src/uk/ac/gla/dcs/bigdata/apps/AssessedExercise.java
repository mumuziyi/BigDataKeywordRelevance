package uk.ac.gla.dcs.bigdata.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import scala.jdk.IntAccumulator;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.ProcessNewsArticle;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.ProcessQuery;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsEssential;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
//
        MyFunctions myFunctions = new MyFunctions(newsFile,queryFile,spark);
        myFunctions.process();


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

//        Map each news article to a NewsEssential object, store them in a new Dataset
        Dataset<NewsEssential> newsEssentialDataset = news.map(new NewsTransformation(), Encoders.bean(NewsEssential.class));
//        Removing all stop words and stemming for each news article and each query
        Dataset<NewsEssential> processedNewsDataset = newsEssentialDataset.map(new NewsWordProcessor(), Encoders.bean(NewsEssential.class));
        Dataset<Query> processedQueries = queries.map(new QueryProcessor(), Encoders.bean(Query.class));

//        Testing
//        processedNewsDataset.foreach((ForeachFunction<NewsEssential>) newsEssential -> {
//            System.out.println(newsEssential.getContents().get(0).getContent());
//        });
//        processedQueries.foreach((ForeachFunction<Query>) query -> {
//            System.out.println(query.getOriginalQuery());
//        });
//        Get the total number of news articles
        long newsCount = newsEssentialDataset.count();
//        Create a list to store the length of each news article
        List<Integer> lengthList = new ArrayList<>();
        newsEssentialDataset.foreach((ForeachFunction<NewsEssential>) newsEssential -> {
            var tmp_length = 0;
            for (var content : newsEssential.getContents()) {
                tmp_length += content.getContent().split(" ").length;
            }
            lengthList.add(tmp_length);
        });
//        Using accumulator to calculate the average length of news articles
        LongAccumulator accumulator = spark.sparkContext().longAccumulator();
        newsEssentialDataset.foreach((ForeachFunction<NewsEssential>) newsEssential -> {
            var tmp_length = 0;
            for (var content : newsEssential.getContents()) {
                tmp_length += content.getContent().split(" ").length;
            }
            accumulator.add(tmp_length);
        });

        double averageLength = accumulator.value() / newsCount;
        LongAccumulator term_accumulator = spark.sparkContext().longAccumulator();
        var freqInCorpus = TermFrequencyCorpus.getTermFrequencyCorpus("finance", processedNewsDataset, term_accumulator);
        System.out.println("Frequency in corpus for finance is " + freqInCorpus);
        System.out.println("Average length is " + averageLength);
//        processedNewsDataset.foreach((ForeachFunction<NewsEssential>) newsEssential -> {
//            double score = DPHScorer.getDPHScore(newsEssential.getQueryFrequency("finance"),freqInCorpus, newsEssential.getLength(), averageLength, processedNewsDataset.count());
//            System.out.println("Score for finance is " + score);
//        });


//        Perform the query execution for every query in the processedQueries Dataset
//


        return null; // replace this with the list of DocumentRanking output by your topology
    }


}
