package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.MyMaps.*;
import uk.ac.gla.dcs.bigdata.MyStructure.NewsCount;
import uk.ac.gla.dcs.bigdata.MyStructure.NewsDPHScore;
import uk.ac.gla.dcs.bigdata.MyStructure.QueryNewsListStructure;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

import static uk.ac.gla.dcs.bigdata.MyMaps.MyFunctions.getAccumulator;

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
		// 广播所有的query terms用于计数
		Broadcast<Set<String>> broadcastTerms = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(terms);

		// 对newsarticle进行map
		Dataset<NewsCount> newsCount = news.map(new NewsToCountMap(broadcastTerms,accumulatorMap,totalLengthInAll,newsNumbInAll),
				Encoders.bean(NewsCount.class));

		newsCount.collectAsList();

//		 得到了所有需要的东西，计算DPH值并且进行map
		Dataset<NewsDPHScore> NewsDphScore = newsCount.map(new CalDPHScoreAndMap(broadcastQuery,accumulatorMap,totalLengthInAll,newsNumbInAll),
				Encoders.bean(NewsDPHScore.class));

		Dataset<QueryNewsListStructure> queryNewsListStructureDataset = NewsDphScore.map(new ToQueryNewsStructure(),Encoders.bean(QueryNewsListStructure.class));

		queryNewsListStructureDataset.collectAsList();

		QueryNewsListStructure finalAnswer = queryNewsListStructureDataset.reduce(new QueryNewsReduce());

		Map<Query, List<RankedResult>> finalAnswerMap= finalAnswer.getQueryListMap();

		for (Query query: finalAnswerMap.keySet()){
			System.out.println("Current query is " + query.getOriginalQuery());
			List<RankedResult> rankedResults = finalAnswerMap.get(query);
			Collections.sort(rankedResults);
			Collections.reverse(rankedResults);
			for (RankedResult rankedResult: rankedResults){
				if (rankedResult.getScore() > 0){
					System.out.println(rankedResult.getArticle().getTitle() + "   " + rankedResult.getScore());
				}
			}
			System.out.println();

		}

//		queryNewsListStructureDataset.collectAsList();



//		// 输出测试
//		List<NewsCount> newsCountList = newsCount.collectAsList();
//		System.out.println(totalLengthInAll);
//		System.out.println(newsNumbInAll);
//		System.out.println(totalLength);
//
//		for (NewsCount newsCount1: newsCountList){
//			Map<String,Integer> count = newsCount1.getTermCountMap();
//			if (newsCount1.getTermCountMap().keySet().size() == 0){
//				continue;
//			}
//			System.out.println(newsCount1.getNewsArticle().getTitle());
//			Set<String> keys = count.keySet();
//			for (String key: keys){
//				System.out.print(key+count.get(key) + "  ");
//			}
//			System.out.println();
//		}
//
//		Set<String> accumulatorSet = accumulatorMap.keySet();
//		for (String key: accumulatorSet){
//			System.out.println(key + "  " + accumulatorMap.get(key));
//		}
//
		
		return null; // replace this with the the list of DocumentRanking output by your topology
	}


	
}
