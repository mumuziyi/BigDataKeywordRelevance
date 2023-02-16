package uk.ac.gla.dcs.bigdata.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import uk.ac.gla.dcs.bigdata.studentfunctions.MyFunctions;

import java.io.File;

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
        SparkConf conf = new SparkConf().setMaster(sparkMasterDef).setAppName(sparkSessionName);

        // Create the spark session
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();


        // Get the location of the input queries
        String queryFile = System.getenv("bigdata.queries");
        if (queryFile == null) queryFile = "data/queries.list"; // default is a sample with 3 queries

        // Get the location of the input news articles
        String newsFile = System.getenv("bigdata.news");
        if (newsFile == null)
//            newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";
            newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles

//
        MyFunctions myFunctions = new MyFunctions(newsFile,queryFile,spark);
        myFunctions.process();


        // Call the student's code
//        List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

        // Close the spark session
        spark.close();


    }

}
