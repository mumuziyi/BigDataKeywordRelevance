package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.util.List;

public class MyFunctions {
    public static void filterStopWords(){

    }
    public static List<String> preProcessing(SparkSession spark, String text, String Queries){
        Dataset<Row> steamGamesAsRowTable = spark
                .read().text(text);

        TextPreProcessor processor = new TextPreProcessor();
        List<String> tokes = processor.process(text);



        return null;
    }
}
