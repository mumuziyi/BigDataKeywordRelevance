package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsCount;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryNewsListStructure;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
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
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
//		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // default is a sample of 5000 news articles

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
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

		List<Query> queryList = queries.collectAsList();

		Set<String> terms = MyFunctions.getTermsSet(queryList);
		Map<String, LongAccumulator> accumulatorMap = MyFunctions.getAccumulator(terms,spark);

		LongAccumulator totalLengthInAll = spark.sparkContext().longAccumulator();
		LongAccumulator newsNumbInAll = spark.sparkContext().longAccumulator();

		Broadcast<List<Query>> broadcastQuery = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryList);
		Broadcast<Set<String>> broadcastTerms = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(terms);

		Dataset<NewsCount> newsCount = news.map(new NewsToCountMap(broadcastTerms,accumulatorMap,totalLengthInAll,newsNumbInAll),
				Encoders.bean(NewsCount.class));

		newsCount.collectAsList();

		Map<String,Long> accumulatorMapLong = new HashMap<>();
		for (String string: accumulatorMap.keySet()){
			accumulatorMapLong.put(string,accumulatorMap.get(string).value());
		}


		Dataset<NewsDPHScore> NewsDphScore = newsCount.map(new CalDPHScoreAndMap(broadcastQuery,
						accumulatorMapLong,totalLengthInAll.value(),newsNumbInAll.value()),
				Encoders.bean(NewsDPHScore.class));


		Dataset<QueryNewsListStructure> queryNewsListStructureDataset = NewsDphScore.map(new ToQueryNewsStructure(),
				Encoders.bean(QueryNewsListStructure.class));

		QueryNewsListStructure finalAnswer = queryNewsListStructureDataset.reduce(new QueryNewsReducer());

		Map<String, List<RankedResult>> finalAnswerMap= finalAnswer.getQueryListMap();


		List<DocumentRanking> documentRankings = new ArrayList<>();

		for (String query: finalAnswerMap.keySet()){

			List<RankedResult> rankedResults = finalAnswerMap.get(query);

			Collections.sort(rankedResults);
			Collections.reverse(rankedResults);

			rankedResults = MyFunctions.getTop10(rankedResults);

			Query certainQuery = queryList.stream().filter(q -> q.getOriginalQuery().equals(query)).findFirst().get();

			documentRankings.add(new DocumentRanking(certainQuery, rankedResults));

		}

		return documentRankings; // replace this with the the list of DocumentRanking output by your topology
//		return null;
	}


	
}
